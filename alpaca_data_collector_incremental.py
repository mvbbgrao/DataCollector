#!/usr/bin/env python3
"""
Alpaca Incremental Data Updater (Fixed UTC Version)

This module handles daily incremental updates after market close.
It fetches only recent data and computes RVOL/EMAs using historical
baselines stored in the database for continuity.

IMPORTANT: All timestamps are stored in UTC. Slot index and session date
calculations convert to ET internally but timestamps remain UTC.

RVOL Implementation (5-minute and 15-minute candles):

5-minute candles:
- slot_index: 0-77 mapping to 5-minute slots from 09:30-16:00 ET (78 slots)
- cum_volume: cumulative volume within session

15-minute candles:
- slot_index: 0-25 mapping to 15-minute slots from 09:30-16:00 ET (26 slots)
- cum_volume: cumulative volume within session

Usage:
    python alpaca_incremental_updater.py [--days 5]
"""

from alpaca.data import TimeFrameUnit
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from numba import njit
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import pytz
import logging
from dotenv import load_dotenv
import os
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import argparse
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
import json

load_dotenv()

# Configuration
PG_DSN = os.getenv("MARKET_DATA_PG_DSN", "dbname=testmarket user=postgres password=postgres host=localhost port=5432")
TICKERS_FILE = "ticker_list.txt"
MARKET_START_PST, MARKET_END_PST = 6.5, 12.99
MAX_WORKERS, BATCH_SIZE, DB_BATCH_SIZE = 20, 10, 100
RVOL_LOOKBACK_SESSIONS, RVOL_MIN_SESSIONS, RVOL_MIN_BASELINE_VOLUME = 20, 5, 100
DEFAULT_INCREMENTAL_DAYS = 5
EMA_HISTORY_BARS = 100

# Slot counts for different timeframes
SLOTS_5MIN = 78  # 6.5 hours * 12 slots/hour
SLOTS_15MIN = 26  # 6.5 hours * 4 slots/hour

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.FileHandler("incremental_updater.log"), logging.StreamHandler()])


@dataclass
class VolumeProfileLevel:
    price_level: float
    volume: int
    percentage: float
    is_poc: bool = False
    is_value_area: bool = False


@dataclass
class VolumeProfileMetrics:
    poc_price: float
    value_area_high: float
    value_area_low: float
    total_volume: int
    price_levels: List[VolumeProfileLevel]


@dataclass
class SessionStats:
    highest_high: float
    session_high: float
    session_low: float
    poc: float
    vwap_upper: float
    vwap_lower: float
    session_volume: int


@dataclass
class CompositeSessionProfile:
    symbol: str
    session_date: date
    lookback_days: int
    composite_poc: float
    composite_vah: float
    composite_val: float
    total_volume: int
    hvn_levels: List[float]
    lvn_levels: List[float]
    imbalance_score: float


@dataclass
class SlotBaseline:
    slot_index: int
    baseline_slot_volume: float
    baseline_cum_volume: float
    baseline_pct_of_day: float
    avg_daily_volume: float


@njit(cache=True)
def _distribute_volume_exact(lows, highs, volumes, bin_lowers, bin_uppers, price_levels):
    volume_at_price = np.zeros(price_levels, dtype=np.float64)
    for i in range(len(volumes)):
        if volumes[i] == 0:
            continue
        bar_low, bar_high, bar_vol = lows[i], highs[i], volumes[i]
        count = sum(1 for j in range(price_levels) if bin_lowers[j] <= bar_high and bin_uppers[j] >= bar_low)
        if count > 0:
            vol_per_bin = bar_vol / count
            for j in range(price_levels):
                if bin_lowers[j] <= bar_high and bin_uppers[j] >= bar_low:
                    volume_at_price[j] += vol_per_bin
    return volume_at_price


@njit(cache=True)
def _calculate_value_area(volume_at_price, price_levels):
    poc_index, max_vol = 0, volume_at_price[0]
    for i in range(1, price_levels):
        if volume_at_price[i] > max_vol:
            max_vol, poc_index = volume_at_price[i], i
    total_volume = volume_at_price.sum()
    value_area_volume = total_volume * 0.70
    current_volume, low_index, high_index = volume_at_price[poc_index], poc_index, poc_index
    while current_volume < value_area_volume:
        expand_low, expand_high = low_index > 0, high_index < price_levels - 1
        if not expand_low and not expand_high:
            break
        low_vol = volume_at_price[low_index - 1] if expand_low else 0.0
        high_vol = volume_at_price[high_index + 1] if expand_high else 0.0
        if expand_high and (not expand_low or high_vol >= low_vol):
            high_index += 1
            current_volume += volume_at_price[high_index]
        elif expand_low:
            low_index -= 1
            current_volume += volume_at_price[low_index]
    return poc_index, low_index, high_index, total_volume


class AlpacaIncrementalUpdater:
    def __init__(self, api_key: str, secret_key: str):
        self.client = StockHistoricalDataClient(api_key, secret_key)
        self.pst_tz = pytz.timezone('US/Pacific')
        self.est_tz = pytz.timezone('US/Eastern')
        self.utc_tz = pytz.UTC
        self.db_lock = threading.Lock()
        self.api_call_count = 0
        self.start_time = None

    def _get_pg_conn(self):
        return psycopg2.connect(PG_DSN)

    def load_tickers(self) -> List[str]:
        if not os.path.exists(TICKERS_FILE):
            logging.error(f"Tickers file {TICKERS_FILE} not found")
            return []
        with open(TICKERS_FILE, "r") as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
        logging.info(f"Loaded {len(tickers)} tickers")
        return tickers

    def is_market_hours(self, timestamp: datetime) -> bool:
        """Check if timestamp is within market hours. Handles both UTC and ET timestamps."""
        if timestamp.tzinfo is None:
            # Assume UTC for naive timestamps from Alpaca
            timestamp = self.utc_tz.localize(timestamp)
        pst_time = timestamp.astimezone(self.pst_tz)
        return MARKET_START_PST <= pst_time.hour + pst_time.minute / 60.0 <= MARKET_END_PST

    def _compute_slot_index_from_utc(self, ts: pd.Timestamp, interval_minutes: int = 15) -> Optional[int]:
        """
        Compute slot_index for regular session (09:30-16:00 ET).
        Input timestamp is in UTC, converts to ET for slot calculation.

        Args:
            ts: Timestamp in UTC
            interval_minutes: 5 or 15 for the candle interval

        Returns:
            slot_index (0-77 for 5min, 0-25 for 15min), or None if outside regular session.
        """
        # Convert UTC to ET for slot calculation
        if ts.tzinfo is None:
            ts_utc = self.utc_tz.localize(ts.to_pydatetime())
        else:
            ts_utc = ts.to_pydatetime()

        ts_et = ts_utc.astimezone(self.est_tz)

        total_min = ts_et.hour * 60 + ts_et.minute
        start = 9 * 60 + 30  # 09:30 ET
        end = 16 * 60  # 16:00 ET

        if total_min < start or total_min >= end:
            return None

        return int((total_min - start) // interval_minutes)

    def _get_session_date_from_utc(self, ts: pd.Timestamp) -> date:
        """
        Get the trading session date from a UTC timestamp.
        Converts to ET to determine the correct trading day.
        """
        if ts.tzinfo is None:
            ts_utc = self.utc_tz.localize(ts.to_pydatetime())
        else:
            ts_utc = ts.to_pydatetime()

        ts_et = ts_utc.astimezone(self.est_tz)
        return ts_et.date()

    def get_historical_candles_from_db(self, symbol: str, table_name: str,
                                       num_bars: int = EMA_HISTORY_BARS) -> pd.DataFrame:
        conn = self._get_pg_conn()
        try:
            df = pd.read_sql(
                f"SELECT symbol, timestamp, open, high, low, close, volume FROM {table_name} WHERE symbol = %s ORDER BY timestamp DESC LIMIT %s",
                conn, params=(symbol, num_bars)
            )
            return df.sort_values("timestamp").reset_index(drop=True) if not df.empty else df
        except Exception as e:
            logging.error(f"Error fetching historical candles for {symbol}: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def get_historical_daily_bars_from_db(self, symbol: str, num_bars: int = EMA_HISTORY_BARS) -> pd.DataFrame:
        conn = self._get_pg_conn()
        try:
            df = pd.read_sql(
                "SELECT symbol, timestamp, open, high, low, close, volume, vwap FROM daily_bars WHERE symbol = %s ORDER BY timestamp DESC LIMIT %s",
                conn, params=(symbol, num_bars)
            )
            return df.sort_values("timestamp").reset_index(drop=True) if not df.empty else df
        except Exception as e:
            logging.error(f"Error fetching historical daily bars for {symbol}: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def get_rvol_baselines_from_db(
            self,
            symbol: str,
            table_name: str,
            interval_minutes: int,
            lookback_sessions: int = RVOL_LOOKBACK_SESSIONS
    ) -> Dict[int, SlotBaseline]:
        """
        Fetch RVOL baselines from historical data in DB.

        Args:
            symbol: Stock symbol
            table_name: 'candles_5min' or 'candles_15min'
            interval_minutes: 5 or 15
            lookback_sessions: Number of sessions for baseline calculation

        Returns:
            Dict mapping slot_index to SlotBaseline
        """
        num_slots = SLOTS_5MIN if interval_minutes == 5 else SLOTS_15MIN

        conn = self._get_pg_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            # Get distinct session dates (using ET date from UTC timestamp)
            cur.execute(f"""
                SELECT DISTINCT (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date as session_date
                FROM {table_name}
                WHERE symbol = %s
                ORDER BY session_date DESC
                LIMIT %s
            """, (symbol, lookback_sessions + 1))
            session_dates = [row['session_date'] for row in cur.fetchall()]

            if len(session_dates) < RVOL_MIN_SESSIONS:
                return {}

            # Exclude most recent session for baseline calculation
            baseline_dates = session_dates[1:lookback_sessions + 1]
            if len(baseline_dates) < RVOL_MIN_SESSIONS:
                return {}

            # Get slot data for baseline dates
            cur.execute(f"""
                SELECT slot_index,
                       volume,
                       cum_volume,
                       (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date as session_date
                FROM {table_name}
                WHERE symbol = %s
                  AND (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date = ANY(%s)
                  AND slot_index IS NOT NULL
                ORDER BY slot_index, session_date
            """, (symbol, baseline_dates))
            rows = cur.fetchall()

            if not rows:
                return {}

            slot_data = {i: [] for i in range(num_slots)}
            for row in rows:
                if row['slot_index'] is not None and row['slot_index'] < num_slots:
                    slot_data[row['slot_index']].append(row)

            # Get daily volumes for baseline dates
            cur.execute(f"""
                SELECT (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date as session_date, 
                       SUM(volume) as daily_volume
                FROM {table_name}
                WHERE symbol = %s
                  AND (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date = ANY(%s)
                  AND slot_index IS NOT NULL
                GROUP BY (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date
            """, (symbol, baseline_dates))
            daily_volumes = {row['session_date']: row['daily_volume'] for row in cur.fetchall()}
            avg_daily_volume = np.mean(list(daily_volumes.values())) if daily_volumes else 0

            baselines = {}
            for slot_idx in range(num_slots):
                slot_rows = slot_data[slot_idx]
                if len(slot_rows) < RVOL_MIN_SESSIONS:
                    continue

                volumes = [r['volume'] for r in slot_rows]
                cum_volumes = [r['cum_volume'] for r in slot_rows if r['cum_volume'] is not None]
                pct_of_days = [
                    r['cum_volume'] / daily_volumes[r['session_date']]
                    for r in slot_rows
                    if r['session_date'] in daily_volumes
                    and daily_volumes[r['session_date']] > 0
                    and r['cum_volume'] is not None
                ]

                baselines[slot_idx] = SlotBaseline(
                    slot_idx,
                    max(float(np.median(volumes)) if volumes else 0, RVOL_MIN_BASELINE_VOLUME),
                    max(float(np.median(cum_volumes)) if cum_volumes else 0, RVOL_MIN_BASELINE_VOLUME),
                    max(float(np.median(pct_of_days)) if pct_of_days else 0, 0.05),
                    max(avg_daily_volume, RVOL_MIN_BASELINE_VOLUME)
                )
            return baselines
        except Exception as e:
            logging.error(f"Error fetching RVOL baselines for {symbol} from {table_name}: {e}")
            return {}
        finally:
            cur.close()
            conn.close()

    def calculate_ema_with_history(self, new_df: pd.DataFrame, historical_df: pd.DataFrame,
                                   periods: List[int]) -> pd.DataFrame:
        """Calculate EMAs using historical data for continuity."""
        if new_df.empty:
            return new_df

        new_df = new_df.copy()
        if historical_df.empty:
            new_df = new_df.sort_values("timestamp").reset_index(drop=True)
            for period in periods:
                new_df[f"ema_{period}"] = new_df["close"].ewm(span=period, adjust=False).mean().round(3)
            return new_df

        historical_df = historical_df.copy()
        new_df["_is_new"] = True
        historical_df["_is_new"] = False
        new_df["timestamp"] = pd.to_datetime(new_df["timestamp"])
        historical_df["timestamp"] = pd.to_datetime(historical_df["timestamp"])

        combined = pd.concat([historical_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["symbol", "timestamp"], keep="last")
        combined = combined.sort_values("timestamp").reset_index(drop=True)

        for period in periods:
            combined[f"ema_{period}"] = combined["close"].ewm(span=period, adjust=False).mean().round(3)

        result = combined[combined["_is_new"] == True].drop(columns=["_is_new"], errors="ignore")
        return result.sort_values("timestamp").reset_index(drop=True)

    def calculate_indicator_with_history(self, new_df: pd.DataFrame, historical_df: pd.DataFrame, indicator_fn,
                                         indicator_name: str) -> pd.DataFrame:
        """Calculate indicators using historical data for continuity."""
        if new_df.empty:
            new_df[indicator_name] = np.nan
            return new_df

        new_df = new_df.copy()
        if historical_df.empty or len(historical_df) < 10:
            new_df = indicator_fn(new_df)
            return new_df

        historical_df = historical_df.copy()
        new_df["_is_new"] = True
        historical_df["_is_new"] = False
        new_df["timestamp"] = pd.to_datetime(new_df["timestamp"])
        historical_df["timestamp"] = pd.to_datetime(historical_df["timestamp"])

        combined = pd.concat([historical_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["symbol", "timestamp"], keep="last")
        combined = combined.sort_values("timestamp").reset_index(drop=True)
        combined = indicator_fn(combined)

        result = combined[combined["_is_new"] == True].drop(columns=["_is_new"], errors="ignore")
        return result.sort_values("timestamp").reset_index(drop=True)

    def _calc_rsi(self, df: pd.DataFrame, period: int = 10) -> pd.DataFrame:
        if df.empty or len(df) < period + 1:
            df["rsi_10"] = np.nan
            return df
        df = df.copy().sort_values("timestamp").reset_index(drop=True)
        df["price_change"] = df["close"].diff()
        df["gain"] = np.where(df["price_change"] > 0, df["price_change"], 0)
        df["loss"] = np.where(df["price_change"] < 0, -df["price_change"], 0)
        rs = df["gain"].rolling(window=period).mean() / df["loss"].rolling(window=period).mean().replace(0, np.nan)
        df["rsi_10"] = (100 - (100 / (1 + rs))).round(2)
        return df.drop(["price_change", "gain", "loss"], axis=1, errors="ignore")

    def _calc_momentum(self, df: pd.DataFrame, period: int = 10) -> pd.DataFrame:
        if df.empty or len(df) < period + 1:
            df["momentum_10"] = np.nan
            return df
        df = df.copy().sort_values("timestamp").reset_index(drop=True)
        df["momentum_10"] = ((df["close"] / df["close"].shift(period)) * 100).round(2)
        return df

    def _calc_atr(self, df: pd.DataFrame, period: int = 10) -> pd.DataFrame:
        if df.empty:
            df["atr"] = np.nan
            return df
        df = df.copy().sort_values("timestamp").reset_index(drop=True)
        df["tr"] = np.maximum(
            df["high"] - df["low"],
            np.maximum(abs(df["high"] - df["close"].shift(1)), abs(df["low"] - df["close"].shift(1)))
        )
        df["atr"] = df["tr"].ewm(span=period, adjust=False).mean().round(4)
        return df.drop(columns=["tr"], errors="ignore")

    def calculate_daily_indicators_with_history(self, new_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        if new_df.empty:
            return new_df
        historical_df = self.get_historical_daily_bars_from_db(symbol, EMA_HISTORY_BARS)
        new_df = self.calculate_indicator_with_history(new_df, historical_df, self._calc_atr, "atr")
        new_df = self.calculate_indicator_with_history(new_df, historical_df, self._calc_rsi, "rsi_10")
        new_df = self.calculate_indicator_with_history(new_df, historical_df, self._calc_momentum, "momentum_10")
        new_df = self.calculate_ema_with_history(new_df, historical_df, [8, 20])
        return new_df

    def calculate_vwap_series(self, df: pd.DataFrame, include_bands: bool = False,
                              multiplier: float = 1) -> pd.DataFrame:
        """Calculate VWAP series. Uses ET date for session grouping but preserves UTC timestamps."""
        if df.empty:
            return df

        df = df.copy().sort_values("timestamp").reset_index(drop=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Ensure UTC
        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
        else:
            df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")

        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["pv"] = df["typical_price"] * df["volume"]

        # Get session date in ET for grouping (internal use only)
        df["_date_et"] = df["timestamp"].dt.tz_convert(self.est_tz).dt.date

        df["cum_pv"] = df.groupby("_date_et")["pv"].cumsum()
        df["cum_vol_vwap"] = df.groupby("_date_et")["volume"].cumsum()
        df["vwap"] = np.where(df["cum_vol_vwap"] > 0, (df["cum_pv"] / df["cum_vol_vwap"]).round(2), 0.0)

        if include_bands:
            df["vwap_upper"] = df["vwap"]
            df["vwap_lower"] = df["vwap"]

            for date_val in df["_date_et"].unique():
                mask = df["_date_et"] == date_val
                date_df = df[mask]
                if len(date_df) == 0:
                    continue

                vwap_vals = date_df["vwap"].values
                tp_vals = date_df["typical_price"].values
                vol_vals = date_df["volume"].values
                upper, lower = [], []

                for i in range(len(date_df)):
                    vol_sum = vol_vals[:i + 1].sum()
                    if vol_sum > 0:
                        weights = vol_vals[:i + 1] / vol_sum
                        std = np.sqrt(np.sum(weights * (tp_vals[:i + 1] - vwap_vals[i]) ** 2))
                        upper.append(vwap_vals[i] + multiplier * std)
                        lower.append(vwap_vals[i] - multiplier * std)
                    else:
                        upper.append(vwap_vals[i])
                        lower.append(vwap_vals[i])

                df.loc[mask, "vwap_upper"] = np.round(upper, 2)
                df.loc[mask, "vwap_lower"] = np.round(lower, 2)

        # Drop internal columns
        df = df.drop(columns=["typical_price", "pv", "cum_pv", "cum_vol_vwap", "_date_et"], errors="ignore")
        return df

    def calculate_rvol_incremental(
            self,
            df: pd.DataFrame,
            symbol: str,
            table_name: str,
            interval_minutes: int
    ) -> pd.DataFrame:
        """
        Calculate RVOL for incremental data using stored baselines.
        Timestamps remain in UTC; session date and slot index computed from UTC->ET conversion.

        Args:
            df: DataFrame with new candle data
            symbol: Stock symbol
            table_name: 'candles_5min' or 'candles_15min'
            interval_minutes: 5 or 15

        Returns:
            DataFrame with RVOL columns added
        """
        if df.empty:
            return df

        df = df.copy().sort_values("timestamp").reset_index(drop=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Ensure UTC
        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
        else:
            df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")

        # Compute session date in ET (for grouping only)
        df["_session_date"] = df["timestamp"].apply(self._get_session_date_from_utc)

        # Compute slot index from UTC timestamp
        df["slot_index"] = df["timestamp"].apply(
            lambda ts: self._compute_slot_index_from_utc(ts, interval_minutes)
        )

        # Filter to regular session only
        df = df[df["slot_index"].notna()].copy()
        if df.empty:
            return df

        df["slot_index"] = df["slot_index"].astype(int)
        df = df.sort_values(["_session_date", "timestamp"]).reset_index(drop=True)
        df["cum_volume"] = df.groupby("_session_date")["volume"].cumsum()

        # Get baselines from DB
        baselines = self.get_rvol_baselines_from_db(symbol, table_name, interval_minutes)

        if not baselines:
            for col in ["rvol_slot_20", "rvol_slot_baseline_20", "rvol_cum_20", "rvol_cum_baseline_20",
                        "intraday_rvol_20", "avg_daily_volume_20", "pct_of_day_typical"]:
                df[col] = np.nan
            df = df.drop(columns=["_session_date"], errors="ignore")
            return df.sort_values("timestamp").reset_index(drop=True)

        def get_baseline(slot_idx, field):
            if slot_idx in baselines:
                return getattr(baselines[slot_idx], field)
            return np.nan

        df["rvol_slot_baseline_20"] = df["slot_index"].apply(lambda x: get_baseline(x, "baseline_slot_volume"))
        df["rvol_cum_baseline_20"] = df["slot_index"].apply(lambda x: get_baseline(x, "baseline_cum_volume"))
        df["avg_daily_volume_20"] = df["slot_index"].apply(lambda x: get_baseline(x, "avg_daily_volume"))
        df["pct_of_day_typical"] = df["slot_index"].apply(lambda x: get_baseline(x, "baseline_pct_of_day"))

        # Calculate RVOL metrics
        df["rvol_slot_20"] = np.where(
            df["rvol_slot_baseline_20"].notna() & (df["rvol_slot_baseline_20"] > 0),
            (df["volume"] / df["rvol_slot_baseline_20"]).round(4),
            np.nan
        )

        df["rvol_cum_20"] = np.where(
            df["rvol_cum_baseline_20"].notna() & (df["rvol_cum_baseline_20"] > 0),
            (df["cum_volume"] / df["rvol_cum_baseline_20"]).round(4),
            np.nan
        )

        projected_eod = np.where(
            df["pct_of_day_typical"].notna() & (df["pct_of_day_typical"] > 0),
            df["cum_volume"] / df["pct_of_day_typical"],
            np.nan
        )

        df["intraday_rvol_20"] = np.where(
            df["avg_daily_volume_20"].notna() & (df["avg_daily_volume_20"] > 0),
            (projected_eod / df["avg_daily_volume_20"]).round(4),
            np.nan
        )

        df["rvol_slot_baseline_20"] = df["rvol_slot_baseline_20"].round(2)
        df["rvol_cum_baseline_20"] = df["rvol_cum_baseline_20"].round(2)
        df["avg_daily_volume_20"] = df["avg_daily_volume_20"].round(2)
        df["pct_of_day_typical"] = df["pct_of_day_typical"].round(4)

        # Drop internal columns
        df = df.drop(columns=["_session_date"], errors="ignore")
        return df.sort_values("timestamp").reset_index(drop=True)

    def calculate_rvol_incremental_5m(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Calculate RVOL for 5-minute candles. Convenience wrapper."""
        return self.calculate_rvol_incremental(df, symbol, "candles_5min", interval_minutes=5)

    def calculate_rvol_incremental_15m(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Calculate RVOL for 15-minute candles. Convenience wrapper."""
        return self.calculate_rvol_incremental(df, symbol, "candles_15min", interval_minutes=15)

    def calculate_volume_profile_optimized(self, df: pd.DataFrame, price_levels: int = 70) -> VolumeProfileMetrics:
        if df.empty:
            raise ValueError("DataFrame is empty")

        high_price, low_price = df["high"].max(), df["low"].min()
        if high_price == low_price:
            total_vol = int(df["volume"].sum())
            return VolumeProfileMetrics(
                round(high_price, 2), round(high_price, 2), round(high_price, 2), total_vol,
                [VolumeProfileLevel(float(high_price), total_vol, 100.0, True, True)] if total_vol > 0 else []
            )

        price_bins = np.linspace(low_price, high_price, price_levels + 1)
        vol_at_price = _distribute_volume_exact(
            df["low"].values.astype(np.float64),
            df["high"].values.astype(np.float64),
            df["volume"].values.astype(np.float64),
            price_bins[:-1], price_bins[1:], price_levels
        )
        poc_idx, low_idx, high_idx, total_vol = _calculate_value_area(vol_at_price, price_levels)

        levels = [
            VolumeProfileLevel(
                float((price_bins[i] + price_bins[i + 1]) / 2),
                int(vol_at_price[i]),
                (float(vol_at_price[i]) / total_vol * 100) if total_vol > 0 else 0,
                i == poc_idx,
                low_idx <= i <= high_idx
            )
            for i in np.where(vol_at_price > 0)[0]
        ]

        return VolumeProfileMetrics(
            round((price_bins[poc_idx] + price_bins[poc_idx + 1]) / 2, 2),
            round((price_bins[high_idx] + price_bins[high_idx + 1]) / 2, 2),
            round((price_bins[low_idx] + price_bins[low_idx + 1]) / 2, 2),
            int(total_vol), levels
        )

    def fetch_recent_intraday_data(self, symbol: str, timeframe: TimeFrame,
                                   days_back: int = DEFAULT_INCREMENTAL_DAYS) -> pd.DataFrame:
        """Fetch recent intraday data from Alpaca. Returns data with UTC timestamps."""
        end_date = datetime.now(self.est_tz)
        start_date = datetime.now(self.est_tz) - timedelta(days=days_back)

        try:
            self.api_call_count += 1
            bars = self.client.get_stock_bars(
                StockBarsRequest(
                    symbol_or_symbols=[symbol],
                    timeframe=timeframe,
                    start=start_date.isoformat(),
                    end=end_date.isoformat()
                )
            )
            df = bars.df.reset_index()
            if df.empty:
                return pd.DataFrame()

            # Alpaca returns UTC timestamps
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Filter to market hours
            df["is_market_hours"] = df["timestamp"].apply(self.is_market_hours)
            df = df[df["is_market_hours"]].copy()
            df = df.drop(columns=["is_market_hours"], errors="ignore")

            return df
        except Exception as e:
            logging.error(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_and_process_session_data(self, symbol: str, dates: List[str]) -> Tuple[
        Dict[str, Optional[SessionStats]], pd.DataFrame]:
        """Fetch and process session data for multiple dates."""
        if not dates:
            return {}, pd.DataFrame()

        dates_as_date = sorted([datetime.strptime(d, "%Y-%m-%d").date() for d in dates])
        est = self.est_tz

        try:
            self.api_call_count += 1
            session_start = est.localize(datetime.combine(dates_as_date[0], datetime.min.time()).replace(hour=4))
            session_end = est.localize(datetime.combine(dates_as_date[-1], datetime.min.time()).replace(hour=20))

            bars = self.client.get_stock_bars(
                StockBarsRequest(
                    symbol_or_symbols=[symbol],
                    timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                    start=session_start.isoformat(),
                    end=session_end.isoformat()
                )
            )
            df = bars.df.reset_index()

            if df.empty:
                return {d.isoformat(): None for d in dates_as_date}, pd.DataFrame()

            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Filter to regular hours (convert to ET for filtering)
            df["_timestamp_et"] = df["timestamp"].dt.tz_convert(est)
            df["_time_dec"] = df["_timestamp_et"].dt.hour + df["_timestamp_et"].dt.minute / 60.0
            df = df[(df["_time_dec"] >= 9.5) & (df["_time_dec"] < 16.0)].copy()

            if df.empty:
                return {d.isoformat(): None for d in dates_as_date}, pd.DataFrame()

            # Get date in ET for grouping
            df["date"] = df["_timestamp_et"].dt.date
            df = df.drop(columns=["_timestamp_et", "_time_dec"], errors="ignore")

            day_dfs = {}
            for d in dates_as_date:
                day_df = df[df["date"] == d].copy()
                if not day_df.empty:
                    day_df = self.calculate_vwap_series(day_df, include_bands=True)
                day_dfs[d] = day_df if not day_df.empty else pd.DataFrame()

        except Exception as e:
            logging.error(f"Failed session data for {symbol}: {e}")
            return {d.isoformat(): None for d in dates_as_date}, pd.DataFrame()

        results = {}
        profiles = []

        for idx, d in enumerate(dates_as_date):
            day_df = day_dfs[d]
            if day_df.empty:
                results[d.isoformat()] = None
                continue

            try:
                vp = self.calculate_volume_profile_optimized(day_df)
                sh, sl, poc = vp.value_area_high, vp.value_area_low, vp.poc_price
            except:
                sh, sl = day_df["high"].max(), day_df["low"].min()
                poc = day_df.loc[day_df["volume"].idxmax(), "close"] if not day_df.empty else None

            results[d.isoformat()] = SessionStats(
                day_df["high"].max(), sh, sl, poc,
                day_df["vwap_upper"].iloc[-1] if "vwap_upper" in day_df.columns else None,
                day_df["vwap_lower"].iloc[-1] if "vwap_lower" in day_df.columns else None,
                day_df["volume"].sum()
            )

            # Build composite profiles
            for lb in [2, 3, 4]:
                if idx - lb >= 0:
                    window_dfs = [day_dfs[dates_as_date[i]] for i in range(idx - lb, idx) if
                                  not day_dfs[dates_as_date[i]].empty]
                    if window_dfs:
                        comp_df = pd.concat(window_dfs, ignore_index=True)
                        try:
                            cvp = self.calculate_volume_profile_optimized(comp_df, 70)
                            sorted_vols = sorted(cvp.price_levels, key=lambda x: x.volume, reverse=True)
                            hvn = [round(v.price_level, 2) for v in sorted_vols[:3]]
                            lvn = [round(v.price_level, 2) for v in
                                   sorted([v for v in cvp.price_levels if v.volume > 0], key=lambda x: x.volume)[:3]]
                            mid = (comp_df["high"].max() + comp_df["low"].min()) / 2
                            width = max(comp_df["high"].max() - comp_df["low"].min(), 1e-6)
                            imb = min(100.0, abs(cvp.poc_price - mid) / width * 200)
                            profiles.append(CompositeSessionProfile(symbol, d, lb, cvp.poc_price, cvp.value_area_high,
                                                                    cvp.value_area_low, cvp.total_volume, hvn, lvn,
                                                                    imb))
                        except:
                            pass

        if profiles:
            self._store_composite_session_profiles_batch(profiles)

        # Build daily bars
        daily_records = []
        for d in sorted(day_dfs.keys()):
            day_df = day_dfs[d]
            if day_df.empty:
                continue

            vol_sum = day_df["volume"].sum()
            typical = (day_df["high"] + day_df["low"] + day_df["close"]) / 3
            vwap = round(float((typical * day_df["volume"]).sum() / vol_sum), 2) if vol_sum > 0 else float(
                day_df["close"].iloc[-1])

            daily_records.append({
                "symbol": symbol,
                "timestamp": pd.Timestamp(d),
                "open": float(day_df["open"].iloc[0]),
                "high": float(day_df["high"].max()),
                "low": float(day_df["low"].min()),
                "close": float(day_df["close"].iloc[-1]),
                "volume": int(vol_sum),
                "vwap": vwap
            })

        daily_df = pd.DataFrame(daily_records).sort_values("timestamp").reset_index(
            drop=True) if daily_records else pd.DataFrame()
        return results, daily_df

    def upsert_candles(self, data_dict: Dict[str, pd.DataFrame], table_name: str):
        """Upsert candle data to database. Ensures timestamps are stored as naive UTC."""
        if not data_dict:
            return

        conn = self._get_pg_conn()
        cur = None
        try:
            cur = conn.cursor()
            all_records = []

            for symbol, df in data_dict.items():
                if not df.empty:
                    df = df.copy()
                    df['timestamp'] = pd.to_datetime(df['timestamp'])

                    # Convert to UTC and remove timezone for storage
                    if df['timestamp'].dt.tz is not None:
                        df['timestamp'] = df['timestamp'].dt.tz_convert('UTC').dt.tz_localize(None)

                    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    all_records.extend(df.to_dict('records'))

            if not all_records:
                return

            columns = list(all_records[0].keys())
            update_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in columns if c not in ("symbol", "timestamp")])

            execute_batch(
                cur,
                f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))}) ON CONFLICT (symbol, timestamp) DO UPDATE SET {update_clause}",
                [[r.get(c) for c in columns] for r in all_records],
                page_size=DB_BATCH_SIZE
            )
            conn.commit()
            logging.info(f"Upserted {len(all_records)} records into {table_name}")
        except Exception as e:
            logging.error(f"Error upserting to {table_name}: {e}")
            conn.rollback()
        finally:
            if cur:
                cur.close()
            conn.close()

    def upsert_daily_bars(self, all_bars: Dict[str, List[dict]]):
        """Upsert daily bar data."""
        if not all_bars:
            return

        conn = self._get_pg_conn()
        cur = None
        try:
            cur = conn.cursor()
            all_records = [bar for bars in all_bars.values() for bar in bars]

            if not all_records:
                return

            columns = [
                "symbol", "timestamp", "open", "high", "low", "close", "volume",
                "vwap", "vwap_upper", "vwap_lower", "session_high", "session_low",
                "poc", "atr", "rsi_10", "momentum_10", "ema_8", "ema_20", "ema_39_of_15min"
            ]
            update_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in columns if c not in ("symbol", "timestamp")])

            execute_batch(
                cur,
                f"INSERT INTO daily_bars ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))}) ON CONFLICT (symbol, timestamp) DO UPDATE SET {update_clause}",
                [[bar.get(c) for c in columns] for bar in all_records],
                page_size=DB_BATCH_SIZE
            )
            conn.commit()
            logging.info(f"Upserted {len(all_records)} daily bars")
        except Exception as e:
            logging.error(f"Error upserting daily bars: {e}")
            conn.rollback()
        finally:
            if cur:
                cur.close()
            conn.close()

    def _store_composite_session_profiles_batch(self, profiles: List[CompositeSessionProfile]):
        """Store composite session profiles."""
        if not profiles:
            return

        conn = self._get_pg_conn()
        cur = None
        try:
            cur = conn.cursor()
            records = [
                (
                    p.symbol, p.session_date, int(p.lookback_days),
                    float(p.composite_poc) if p.composite_poc else None,
                    float(p.composite_vah) if p.composite_vah else None,
                    float(p.composite_val) if p.composite_val else None,
                    int(p.total_volume) if p.total_volume else None,
                    json.dumps([float(x) for x in (p.hvn_levels or [])]),
                    json.dumps([float(x) for x in (p.lvn_levels or [])]),
                    float(p.imbalance_score) if p.imbalance_score else None
                )
                for p in profiles
            ]
            execute_batch(
                cur,
                """INSERT INTO composite_session_profiles
                   (symbol, session_date, lookback_days, composite_poc, composite_vah, composite_val,
                    total_volume, hvn_levels, lvn_levels, imbalance_score)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (symbol, session_date, lookback_days) DO
                UPDATE SET
                    composite_poc=EXCLUDED.composite_poc, composite_vah=EXCLUDED.composite_vah,
                    composite_val=EXCLUDED.composite_val, total_volume=EXCLUDED.total_volume,
                    hvn_levels=EXCLUDED.hvn_levels, lvn_levels=EXCLUDED.lvn_levels,
                    imbalance_score=EXCLUDED.imbalance_score""",
                records, page_size=1000
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"Error storing composite profiles: {e}")
        finally:
            if cur:
                cur.close()
            conn.close()

    def update_daily_bars_with_ema39_of_15min(self, symbols: List[str]):
        """Update daily bars with EMA39 from 15min candles."""
        if not symbols:
            return

        conn = self._get_pg_conn()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute("""
                        WITH last_ema AS (SELECT DISTINCT
                        ON (symbol, trade_date)
                            symbol, trade_date, ema_39
                        FROM (
                            SELECT symbol, timestamp :: date AS trade_date, ema_39, timestamp
                            FROM candles_15min
                            WHERE symbol = ANY (%s) AND ema_39 IS NOT NULL
                            ) t
                        ORDER BY symbol, trade_date, timestamp DESC
                            )
                        UPDATE daily_bars d
                        SET ema_39_of_15min = l.ema_39 FROM last_ema l
                        WHERE d.symbol = l.symbol AND d.timestamp = l.trade_date
                        """, (symbols,))
            conn.commit()
            logging.info(f"Updated ema_39_of_15min for {len(symbols)} symbols")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating ema_39_of_15min: {e}")
        finally:
            if cur:
                cur.close()
            conn.close()

    def process_symbol_incremental(self, symbol: str, days_back: int = DEFAULT_INCREMENTAL_DAYS) -> Dict[str, any]:
        """Process a single symbol for incremental update."""
        result = {
            "symbol": symbol,
            "daily_bars": [],
            "candles_5min": pd.DataFrame(),
            "candles_15min": pd.DataFrame()
        }

        try:
            self.api_call_count += 1
            bars = self.client.get_stock_bars(
                StockBarsRequest(
                    symbol_or_symbols=[symbol],
                    timeframe=TimeFrame(1, TimeFrameUnit.Day),
                    start=(datetime.now() - timedelta(days=days_back)).isoformat(),
                    end=datetime.now().isoformat(),
                    feed="iex"
                )
            )
            df = bars.df.reset_index()

            if df.empty:
                return result

            df["timestamp"] = pd.to_datetime(df["timestamp"])
            dates_to_fetch = [row["timestamp"].date().isoformat() for _, row in df.sort_values("timestamp").iterrows()]

            session_stats, daily_df = self.fetch_and_process_session_data(symbol, dates_to_fetch)

            if not daily_df.empty:
                daily_df = self.calculate_daily_indicators_with_history(daily_df, symbol)

                for _, row in daily_df.iterrows():
                    stats = session_stats.get(row["timestamp"].date().isoformat())
                    result["daily_bars"].append({
                        "symbol": row["symbol"],
                        "timestamp": row["timestamp"].date().isoformat(),
                        "open": float(row["open"]) if pd.notna(row["open"]) else None,
                        "high": float(stats.highest_high) if stats else float(row["high"]) if pd.notna(
                            row["high"]) else None,
                        "low": float(row["low"]) if pd.notna(row["low"]) else None,
                        "close": float(row["close"]) if pd.notna(row["close"]) else None,
                        "volume": int(stats.session_volume) if stats else 0,
                        "vwap": float(row.get("vwap")) if pd.notna(row.get("vwap")) else None,
                        "vwap_upper": float(stats.vwap_upper) if stats and stats.vwap_upper else None,
                        "vwap_lower": float(stats.vwap_lower) if stats and stats.vwap_lower else None,
                        "session_high": float(stats.session_high) if stats else None,
                        "session_low": float(stats.session_low) if stats else None,
                        "poc": float(stats.poc) if stats else None,
                        "atr": float(row.get("atr")) if pd.notna(row.get("atr")) else None,
                        "rsi_10": float(row.get("rsi_10")) if pd.notna(row.get("rsi_10")) else None,
                        "momentum_10": float(row.get("momentum_10")) if pd.notna(row.get("momentum_10")) else None,
                        "ema_8": float(row.get("ema_8")) if pd.notna(row.get("ema_8")) else None,
                        "ema_20": float(row.get("ema_20")) if pd.notna(row.get("ema_20")) else None
                    })

            # Process 5min data (now with RVOL)
            data_5min = self.fetch_recent_intraday_data(symbol, TimeFrame(5, TimeFrameUnit.Minute), days_back)
            if not data_5min.empty:
                data_5min = self.calculate_vwap_series(data_5min)
                historical_5min = self.get_historical_candles_from_db(symbol, "candles_5min", EMA_HISTORY_BARS)
                data_5min = self.calculate_ema_with_history(data_5min, historical_5min, [8, 20, 39])

                # Calculate RVOL for 5min candles
                data_5min = self.calculate_rvol_incremental_5m(data_5min, symbol)

                # Remove timezone before storing
                if data_5min["timestamp"].dt.tz is not None:
                    data_5min["timestamp"] = data_5min["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)

                # Include RVOL columns for 5min
                cols = [
                    "symbol", "timestamp", "open", "high", "low", "close", "volume", "vwap",
                    "ema_8", "ema_20", "ema_39",
                    "slot_index", "cum_volume",
                    "rvol_slot_20", "rvol_slot_baseline_20",
                    "rvol_cum_20", "rvol_cum_baseline_20",
                    "intraday_rvol_20", "avg_daily_volume_20", "pct_of_day_typical"
                ]
                result["candles_5min"] = data_5min[[c for c in cols if c in data_5min.columns]]

            # Process 15min data
            data_15min = self.fetch_recent_intraday_data(symbol, TimeFrame(15, TimeFrameUnit.Minute), days_back)
            if not data_15min.empty:
                data_15min = self.calculate_vwap_series(data_15min)
                historical_15min = self.get_historical_candles_from_db(symbol, "candles_15min", EMA_HISTORY_BARS)
                data_15min = self.calculate_ema_with_history(data_15min, historical_15min, [8, 20, 39])
                data_15min = self.calculate_rvol_incremental_15m(data_15min, symbol)

                # Remove timezone before storing
                if data_15min["timestamp"].dt.tz is not None:
                    data_15min["timestamp"] = data_15min["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)

                cols = [
                    "symbol", "timestamp", "open", "high", "low", "close", "volume", "vwap",
                    "ema_8", "ema_20", "ema_39", "slot_index", "cum_volume",
                    "rvol_slot_20", "rvol_slot_baseline_20", "rvol_cum_20", "rvol_cum_baseline_20",
                    "intraday_rvol_20", "avg_daily_volume_20", "pct_of_day_typical"
                ]
                result["candles_15min"] = data_15min[[c for c in cols if c in data_15min.columns]]

        except Exception as e:
            logging.error(f"Error processing {symbol}: {e}")

        return result

    def process_batch_incremental(self, symbols: List[str], days_back: int = DEFAULT_INCREMENTAL_DAYS) -> Dict[
        str, Dict]:
        """Process a batch of symbols in parallel."""
        results = {}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self.process_symbol_incremental, s, days_back): s for s in symbols}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    results[symbol] = future.result()
                    logging.info(f"Processed {symbol}")
                except Exception as e:
                    logging.error(f"Error processing {symbol}: {e}")
                    results[symbol] = {
                        "symbol": symbol,
                        "daily_bars": [],
                        "candles_5min": pd.DataFrame(),
                        "candles_15min": pd.DataFrame()
                    }

        return results

    def run_incremental_update(self, days_back: int = DEFAULT_INCREMENTAL_DAYS):
        """Run the full incremental update process."""
        self.start_time = time.time()
        logging.info(f"Starting incremental update (last {days_back} days)")

        tickers = self.load_tickers()
        if not tickers:
            logging.error("No tickers to process")
            return

        logging.info(f"Processing {len(tickers)} tickers in batches of {BATCH_SIZE}")

        for i in range(0, len(tickers), BATCH_SIZE):
            batch = tickers[i:i + BATCH_SIZE]
            batch_start = time.time()
            logging.info(f"Processing batch {i // BATCH_SIZE + 1}/{(len(tickers) - 1) // BATCH_SIZE + 1}: {batch}")

            results = self.process_batch_incremental(batch, days_back)

            daily_dict, c5_dict, c15_dict = {}, {}, {}
            for symbol, data in results.items():
                if data["daily_bars"]:
                    daily_dict[symbol] = data["daily_bars"]
                if not data["candles_5min"].empty:
                    c5_dict[symbol] = data["candles_5min"]
                if not data["candles_15min"].empty:
                    c15_dict[symbol] = data["candles_15min"]

            self.upsert_daily_bars(daily_dict)
            self.upsert_candles(c5_dict, "candles_5min")
            self.upsert_candles(c15_dict, "candles_15min")
            self.update_daily_bars_with_ema39_of_15min(batch)

            logging.info(f"Batch completed in {time.time() - batch_start:.2f} seconds")

        total_time = time.time() - self.start_time
        logging.info(f"Incremental update completed in {total_time:.2f} seconds")
        logging.info(
            f"Total API calls: {self.api_call_count}, Avg per ticker: {self.api_call_count / len(tickers):.2f}")

    def get_update_summary(self):
        """Print summary of today's data."""
        conn = self._get_pg_conn()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute("""
                        SELECT 'candles_15min', COUNT(*), COUNT(DISTINCT symbol)
                        FROM candles_15min
                        WHERE timestamp ::date = CURRENT_DATE
                        UNION ALL
                        SELECT 'candles_5min', COUNT(*), COUNT(DISTINCT symbol)
                        FROM candles_5min
                        WHERE timestamp ::date = CURRENT_DATE
                        UNION ALL
                        SELECT 'daily_bars', COUNT(*), COUNT(DISTINCT symbol)
                        FROM daily_bars
                        WHERE timestamp = CURRENT_DATE
                        """)

            print("\n=== Today's Data Summary ===")
            for row in cur.fetchall():
                print(f"{row[0]}: {row[1]} records, {row[2]} symbols")

            # RVOL Quality for 5min candles
            cur.execute("""
                        SELECT COUNT(*),
                               COUNT(rvol_slot_20),
                               COUNT(intraday_rvol_20),
                               ROUND(AVG(rvol_slot_20)::numeric, 2),
                               ROUND(AVG(intraday_rvol_20)::numeric, 2),
                               COUNT(ema_8),
                               COUNT(ema_39)
                        FROM candles_5min
                        WHERE timestamp ::date = CURRENT_DATE
                        """)
            row = cur.fetchone()

            if row and row[0] > 0:
                print(f"\nQuality (today's 5min):")
                print(f"  Total: {row[0]}")
                print(f"  SlotRVOL: {row[1]} ({100 * row[1] / row[0]:.1f}%)")
                print(f"  IntradayRVOL: {row[2]} ({100 * row[2] / row[0]:.1f}%)")
                print(f"  AvgSlot: {row[3]}, AvgIntraday: {row[4]}")
                print(f"  EMA8: {row[5]} ({100 * row[5] / row[0]:.1f}%)")
                print(f"  EMA39: {row[6]} ({100 * row[6] / row[0]:.1f}%)")

            # RVOL Quality for 15min candles
            cur.execute("""
                        SELECT COUNT(*),
                               COUNT(rvol_slot_20),
                               COUNT(intraday_rvol_20),
                               ROUND(AVG(rvol_slot_20)::numeric, 2),
                               ROUND(AVG(intraday_rvol_20)::numeric, 2),
                               COUNT(ema_8),
                               COUNT(ema_39)
                        FROM candles_15min
                        WHERE timestamp ::date = CURRENT_DATE
                        """)
            row = cur.fetchone()

            if row and row[0] > 0:
                print(f"\nQuality (today's 15min):")
                print(f"  Total: {row[0]}")
                print(f"  SlotRVOL: {row[1]} ({100 * row[1] / row[0]:.1f}%)")
                print(f"  IntradayRVOL: {row[2]} ({100 * row[2] / row[0]:.1f}%)")
                print(f"  AvgSlot: {row[3]}, AvgIntraday: {row[4]}")
                print(f"  EMA8: {row[5]} ({100 * row[5] / row[0]:.1f}%)")
                print(f"  EMA39: {row[6]} ({100 * row[6] / row[0]:.1f}%)")

        finally:
            if cur:
                cur.close()
            conn.close()


def main():
    parser = argparse.ArgumentParser(description="Alpaca Incremental Data Updater")
    parser.add_argument("--days", type=int, default=DEFAULT_INCREMENTAL_DAYS,
                        help=f"Days to fetch (default: {DEFAULT_INCREMENTAL_DAYS})")
    args = parser.parse_args()

    API_KEY = os.getenv("APCA_API_KEY_ID")
    SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

    if not API_KEY or not SECRET_KEY:
        print("Error: Set APCA_API_KEY_ID and APCA_API_SECRET_KEY")
        return

    try:
        updater = AlpacaIncrementalUpdater(API_KEY, SECRET_KEY)
        start = time.time()
        updater.run_incremental_update(days_back=args.days)
        updater.get_update_summary()
        print(f"\n✅ Total: {time.time() - start:.2f}s ({(time.time() - start) / 60:.2f}min)")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()