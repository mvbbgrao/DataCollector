#!/usr/bin/env python3
"""
Alpaca Incremental Data Updater (Fixed UTC Version)

This module handles daily incremental updates after market close.
It fetches only recent data and computes RVOL/EMAs using historical
baselines stored in the database for continuity.

IMPORTANT: All timestamps are stored in UTC. Slot index and session date
calculations convert to ET internally but timestamps remain UTC.

NEW: First Hour 1-Minute Candles (candles_1min_first_hour)
- Captures 9:30-10:30 AM ET (60 one-minute bars)
- slot_index: 0-59 mapping to each minute of the first hour
- Includes RVOL calculations and EMA indicators
- Critical for Initial Balance (IB) analysis and early session momentum

RVOL Implementation:

5-minute candles:
- slot_index: 0-77 mapping to 5-minute slots from 09:30-16:00 ET (78 slots)

15-minute candles:
- slot_index: 0-25 mapping to 15-minute slots from 09:30-16:00 ET (26 slots)

1-minute first hour candles:
- slot_index: 0-59 mapping to 1-minute slots from 09:30-10:30 ET (60 slots)

Daily Bars TrendEngine Indicators:
- ema_39, ema_50: Exponential Moving Averages
- ema_39_slope, ema_50_slope: 3-day slopes
- adx: Average Directional Index (14-period, Wilder smoothing)
- adx_sma: SMA of ADX (5-period)
- adx_slope: 3-day slope of ADX
- di_plus, di_minus: Directional Indicators (+DI/-DI)

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
DEFAULT_INCREMENTAL_DAYS = 7
EMA_HISTORY_BARS = 100

# Slot counts for different timeframes
SLOTS_5MIN = 78  # 6.5 hours * 12 slots/hour
SLOTS_15MIN = 26  # 6.5 hours * 4 slots/hour
SLOTS_1MIN_FIRST_HOUR = 60  # 60 minutes in first hour

# First hour window (ET)
FIRST_HOUR_START_MINUTES = 9 * 60 + 30  # 9:30 AM ET = 570 minutes
FIRST_HOUR_END_MINUTES = 10 * 60 + 30  # 10:30 AM ET = 630 minutes

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
    """Baseline data for a single slot index."""
    slot_index: int
    median_volume: float
    median_cum_volume: float
    pct_of_day_typical: float
    avg_daily_volume: float


@dataclass
class FirstHourSlotBaseline:
    """Baseline data for first hour slot (no pct_of_day needed)."""
    slot_index: int
    median_volume: float
    median_cum_volume: float


@njit(cache=True)
def _distribute_volume_exact(lows, highs, volumes, bin_lowers, bin_uppers, price_levels):
    volume_at_price = np.zeros(price_levels, dtype=np.float64)
    for i in range(len(volumes)):
        if volumes[i] == 0:
            continue
        bar_low, bar_high, bar_vol = lows[i], highs[i], volumes[i]
        count = 0
        for j in range(price_levels):
            if bin_lowers[j] <= bar_high and bin_uppers[j] >= bar_low:
                count += 1
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
            max_vol = volume_at_price[i]
            poc_index = i
    total_volume = volume_at_price.sum()
    value_area_volume = total_volume * 0.70
    current_volume = volume_at_price[poc_index]
    low_index = high_index = poc_index
    while current_volume < value_area_volume:
        expand_low = low_index > 0
        expand_high = high_index < price_levels - 1
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
        if timestamp.tzinfo is None:
            timestamp = self.est_tz.localize(timestamp)
        pst_time = timestamp.astimezone(self.pst_tz)
        hour_decimal = pst_time.hour + pst_time.minute / 60.0
        return MARKET_START_PST <= hour_decimal <= MARKET_END_PST

    def is_first_hour(self, timestamp: datetime) -> bool:
        """Check if timestamp is within first hour (9:30-10:30 AM ET)."""
        if timestamp.tzinfo is None:
            timestamp = self.utc_tz.localize(timestamp)
        et_time = timestamp.astimezone(self.est_tz)
        total_minutes = et_time.hour * 60 + et_time.minute
        return FIRST_HOUR_START_MINUTES <= total_minutes < FIRST_HOUR_END_MINUTES

    def _compute_slot_index_from_utc(self, ts: pd.Timestamp, interval_minutes: int = 15) -> Optional[int]:
        """Compute slot_index from UTC timestamp."""
        if ts.tzinfo is None:
            ts_utc = self.utc_tz.localize(ts.to_pydatetime())
        else:
            ts_utc = ts.to_pydatetime()
        ts_et = ts_utc.astimezone(self.est_tz)
        total_min = ts_et.hour * 60 + ts_et.minute
        start, end = 9 * 60 + 30, 16 * 60
        if total_min < start or total_min >= end:
            return None
        return int((total_min - start) // interval_minutes)

    def _compute_first_hour_slot_index(self, ts: pd.Timestamp) -> Optional[int]:
        """
        Compute slot_index for first hour (09:30-10:30 ET).
        Returns 0-59 for the 60 one-minute slots.
        """
        if ts.tzinfo is None:
            ts_utc = self.utc_tz.localize(ts.to_pydatetime())
        else:
            ts_utc = ts.to_pydatetime()

        ts_et = ts_utc.astimezone(self.est_tz)
        total_min = ts_et.hour * 60 + ts_et.minute

        if total_min < FIRST_HOUR_START_MINUTES or total_min >= FIRST_HOUR_END_MINUTES:
            return None

        return int(total_min - FIRST_HOUR_START_MINUTES)

    def _get_session_date_from_utc(self, ts: pd.Timestamp) -> date:
        """Get trading session date from UTC timestamp."""
        if ts.tzinfo is None:
            ts_utc = self.utc_tz.localize(ts.to_pydatetime())
        else:
            ts_utc = ts.to_pydatetime()
        ts_et = ts_utc.astimezone(self.est_tz)
        return ts_et.date()

    def get_historical_candles_from_db(self, symbol: str, table_name: str, limit: int = 100) -> pd.DataFrame:
        """Fetch recent historical candles from DB for EMA continuity."""
        conn = self._get_pg_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute(f"""
                SELECT symbol, timestamp, open, high, low, close, volume, vwap, 
                       ema_8, ema_20, ema_39, ema_50
                FROM {table_name}
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT %s
            """, (symbol, limit))
            rows = cur.fetchall()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df.sort_values("timestamp").reset_index(drop=True)
        except Exception as e:
            logging.error(f"Error fetching historical candles for {symbol}: {e}")
            return pd.DataFrame()
        finally:
            cur.close()
            conn.close()

    def get_historical_daily_bars_from_db(self, symbol: str, limit: int = 100) -> pd.DataFrame:
        """Fetch recent historical daily bars from DB for indicator continuity."""
        conn = self._get_pg_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute("""
                        SELECT symbol, timestamp, open, high, low, close, volume, vwap, atr, rsi_10, momentum_10, ema_8, ema_20, ema_39, ema_50, adx, adx_sma, di_plus, di_minus
                        FROM daily_bars
                        WHERE symbol = %s
                        ORDER BY timestamp DESC
                            LIMIT %s
                        """, (symbol, limit))
            rows = cur.fetchall()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df.sort_values("timestamp").reset_index(drop=True)
        except Exception as e:
            logging.error(f"Error fetching historical daily bars for {symbol}: {e}")
            return pd.DataFrame()
        finally:
            cur.close()
            conn.close()

    def get_rvol_baselines_from_db(self, symbol: str, table_name: str, num_slots: int) -> Dict[int, SlotBaseline]:
        """Fetch RVOL baselines from historical data in DB."""
        conn = self._get_pg_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            # Get unique session dates (last N sessions)
            cur.execute(f"""
                SELECT DISTINCT (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date as session_date
                FROM {table_name}
                WHERE symbol = %s AND slot_index IS NOT NULL
                ORDER BY session_date DESC
                LIMIT %s
            """, (symbol, RVOL_LOOKBACK_SESSIONS + 1))

            session_dates = [row['session_date'] for row in cur.fetchall()]
            if len(session_dates) <= 1:
                return {}

            # Exclude most recent session (current day)
            baseline_dates = session_dates[1:RVOL_LOOKBACK_SESSIONS + 1]

            # Fetch slot data for baseline dates
            cur.execute(f"""
                SELECT slot_index, volume, cum_volume,
                       (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date as session_date
                FROM {table_name}
                WHERE symbol = %s
                  AND (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date = ANY(%s)
                  AND slot_index IS NOT NULL
                ORDER BY session_date, slot_index
            """, (symbol, baseline_dates))

            slot_data = {i: [] for i in range(num_slots)}
            for row in cur.fetchall():
                slot_data[row['slot_index']].append(row)

            # Get daily volumes for pct_of_day calculation
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

    def get_first_hour_rvol_baselines_from_db(self, symbol: str) -> Dict[int, FirstHourSlotBaseline]:
        """Fetch RVOL baselines for first hour 1-minute candles from DB."""
        conn = self._get_pg_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            # Get unique session dates (last N sessions)
            cur.execute("""
                        SELECT DISTINCT (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') ::date as session_date
                        FROM candles_1min_first_hour
                        WHERE symbol = %s
                          AND slot_index IS NOT NULL
                        ORDER BY session_date DESC
                            LIMIT %s
                        """, (symbol, RVOL_LOOKBACK_SESSIONS + 1))

            session_dates = [row['session_date'] for row in cur.fetchall()]
            if len(session_dates) <= 1:
                return {}

            # Exclude most recent session
            baseline_dates = session_dates[1:RVOL_LOOKBACK_SESSIONS + 1]

            # Fetch slot data for baseline dates
            cur.execute("""
                        SELECT slot_index,
                               volume,
                               cum_volume,
                               (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') ::date as session_date
                        FROM candles_1min_first_hour
                        WHERE symbol = %s
                          AND (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date = ANY(%s)
                          AND slot_index IS NOT NULL
                        ORDER BY session_date, slot_index
                        """, (symbol, baseline_dates))

            slot_data = {i: [] for i in range(SLOTS_1MIN_FIRST_HOUR)}
            for row in cur.fetchall():
                if row['slot_index'] is not None and 0 <= row['slot_index'] < SLOTS_1MIN_FIRST_HOUR:
                    slot_data[row['slot_index']].append(row)

            baselines = {}
            for slot_idx in range(SLOTS_1MIN_FIRST_HOUR):
                slot_rows = slot_data[slot_idx]
                if len(slot_rows) < RVOL_MIN_SESSIONS:
                    continue

                volumes = [r['volume'] for r in slot_rows]
                cum_volumes = [r['cum_volume'] for r in slot_rows if r['cum_volume'] is not None]

                baselines[slot_idx] = FirstHourSlotBaseline(
                    slot_idx,
                    max(float(np.median(volumes)) if volumes else 0, RVOL_MIN_BASELINE_VOLUME),
                    max(float(np.median(cum_volumes)) if cum_volumes else 0, RVOL_MIN_BASELINE_VOLUME)
                )
            return baselines
        except Exception as e:
            logging.error(f"Error fetching first hour RVOL baselines for {symbol}: {e}")
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
        df["_chg"] = df["close"].diff()
        df["_gain"] = np.where(df["_chg"] > 0, df["_chg"], 0)
        df["_loss"] = np.where(df["_chg"] < 0, -df["_chg"], 0)
        avg_gain = df["_gain"].rolling(window=period).mean()
        avg_loss = df["_loss"].rolling(window=period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df["rsi_10"] = (100 - (100 / (1 + rs))).round(2)
        return df.drop(columns=["_chg", "_gain", "_loss"], errors="ignore")

    def _calc_momentum(self, df: pd.DataFrame, period: int = 10) -> pd.DataFrame:
        if df.empty or len(df) < period + 1:
            df["momentum_10"] = np.nan
            return df
        df = df.copy().sort_values("timestamp").reset_index(drop=True)
        df["momentum_10"] = ((df["close"] / df["close"].shift(period)) * 100).round(2)
        return df

    def _calc_atr(self, df: pd.DataFrame, period: int = 10) -> pd.DataFrame:
        if df.empty or len(df) < 2:
            df["atr"] = np.nan
            return df
        df = df.copy().sort_values("timestamp").reset_index(drop=True)
        df["tr"] = np.maximum(
            df["high"] - df["low"],
            np.maximum(abs(df["high"] - df["close"].shift(1)), abs(df["low"] - df["close"].shift(1)))
        )
        df["atr"] = df["tr"].ewm(alpha=1 / period, adjust=False).mean().round(4)  # RMA (TradingView)
        return df.drop(columns=["tr"], errors="ignore")

    def _calc_adx(self, df: pd.DataFrame, length: int = 14, avg_length: int = 5) -> pd.DataFrame:
        """Calculate DI+, DI-, ADX, and SMA(ADX) using Wilder-style smoothing."""
        if df.empty or len(df) < 2:
            df["di_plus"] = np.nan
            df["di_minus"] = np.nan
            df["adx"] = np.nan
            df["adx_sma"] = np.nan
            return df

        df = df.copy()
        df = df.sort_values("timestamp").reset_index(drop=True)

        n = len(df)
        di_plus_arr = np.full(n, np.nan)
        di_minus_arr = np.full(n, np.nan)
        adx_arr = np.full(n, np.nan)
        adx_sma_arr = np.full(n, np.nan)

        smoothed_tr = 0.0
        smoothed_dm_plus = 0.0
        smoothed_dm_minus = 0.0
        dx_values = []
        valid_adx_values = []

        for i in range(n):
            if i == 0:
                continue

            high = df["high"].iloc[i]
            low = df["low"].iloc[i]
            prev_high = df["high"].iloc[i - 1]
            prev_low = df["low"].iloc[i - 1]
            prev_close = df["close"].iloc[i - 1]

            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            true_range = max(tr1, tr2, tr3)

            up_move = high - prev_high
            down_move = prev_low - low
            dm_plus = up_move if (up_move > 0 and up_move > down_move) else 0.0
            dm_minus = down_move if (down_move > 0 and down_move > up_move) else 0.0

            smoothed_tr = smoothed_tr - (smoothed_tr / length) + true_range
            smoothed_dm_plus = smoothed_dm_plus - (smoothed_dm_plus / length) + dm_plus
            smoothed_dm_minus = smoothed_dm_minus - (smoothed_dm_minus / length) + dm_minus

            if smoothed_tr == 0:
                di_plus = 0.0
                di_minus = 0.0
            else:
                di_plus = (smoothed_dm_plus / smoothed_tr) * 100.0
                di_minus = (smoothed_dm_minus / smoothed_tr) * 100.0

            di_plus_arr[i] = round(di_plus, 2)
            di_minus_arr[i] = round(di_minus, 2)

            sum_di = di_plus + di_minus
            dx = 0.0 if sum_di == 0 else (abs(di_plus - di_minus) / sum_di) * 100.0
            dx_values.append(dx)

            if len(dx_values) < length:
                adx = None
            else:
                window = dx_values[-length:]
                adx = sum(window) / float(length)
                adx_arr[i] = round(adx, 2)

            if adx is not None:
                valid_adx_values.append(adx)
                if len(valid_adx_values) >= avg_length:
                    sma_window = valid_adx_values[-avg_length:]
                    adx_sma = sum(sma_window) / float(avg_length)
                    adx_sma_arr[i] = round(adx_sma, 2)

        df["di_plus"] = di_plus_arr
        df["di_minus"] = di_minus_arr
        df["adx"] = adx_arr
        df["adx_sma"] = adx_sma_arr

        return df

    def _calc_slopes(self, df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
        """Calculate slopes for EMA39, EMA50, and ADX using a rolling window."""
        if df.empty:
            df["ema_39_slope"] = np.nan
            df["ema_50_slope"] = np.nan
            df["adx_slope"] = np.nan
            return df

        df = df.copy()

        if "ema_39" in df.columns:
            df["ema_39_slope"] = ((df["ema_39"] - df["ema_39"].shift(window)) / window).round(4)
        else:
            df["ema_39_slope"] = np.nan

        if "ema_50" in df.columns:
            df["ema_50_slope"] = ((df["ema_50"] - df["ema_50"].shift(window)) / window).round(4)
        else:
            df["ema_50_slope"] = np.nan

        if "adx" in df.columns:
            df["adx_slope"] = ((df["adx"] - df["adx"].shift(window)) / window).round(4)
        else:
            df["adx_slope"] = np.nan

        return df

    def _calc_intraday_slopes(self, df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
        """Calculate slopes for EMA39 and EMA50 for intraday candles."""
        if df.empty:
            df["ema_39_slope"] = np.nan
            df["ema_50_slope"] = np.nan
            return df

        df = df.copy()

        if "ema_39" in df.columns:
            df["ema_39_slope"] = ((df["ema_39"] - df["ema_39"].shift(window)) / window).round(4)
        else:
            df["ema_39_slope"] = np.nan

        if "ema_50" in df.columns:
            df["ema_50_slope"] = ((df["ema_50"] - df["ema_50"].shift(window)) / window).round(4)
        else:
            df["ema_50_slope"] = np.nan

        return df

    def _calc_intraday_atr(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """
        Calculate ATR for intraday candles with configurable period.

        Args:
            df: DataFrame with OHLC data
            period: ATR period (8 for 15-min, 12 for 5-min)

        Returns:
            DataFrame with 'atr' column added
        """
        if df.empty or len(df) < 2:
            df["atr"] = np.nan
            return df

        df = df.copy()
        df = df.sort_values("timestamp").reset_index(drop=True)

        df["tr"] = np.maximum(
            df["high"] - df["low"],
            np.maximum(abs(df["high"] - df["close"].shift(1)), abs(df["low"] - df["close"].shift(1)))
        )
        df["atr"] = df["tr"].ewm(alpha=1 / period, adjust=False).mean().round(4)  # RMA (TradingView)

        return df.drop(columns=["tr"], errors="ignore")

    def calculate_intraday_atr_with_history(self, new_df: pd.DataFrame, historical_df: pd.DataFrame,
                                            period: int = 14) -> pd.DataFrame:
        """Calculate intraday ATR using historical data for continuity."""
        if new_df.empty:
            new_df["atr"] = np.nan
            return new_df

        new_df = new_df.copy()
        if historical_df.empty or len(historical_df) < period:
            return self._calc_intraday_atr(new_df, period)

        historical_df = historical_df.copy()
        new_df["_is_new"] = True
        historical_df["_is_new"] = False
        new_df["timestamp"] = pd.to_datetime(new_df["timestamp"])
        historical_df["timestamp"] = pd.to_datetime(historical_df["timestamp"])

        combined = pd.concat([historical_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["symbol", "timestamp"], keep="last")
        combined = combined.sort_values("timestamp").reset_index(drop=True)

        combined = self._calc_intraday_atr(combined, period)

        result = combined[combined["_is_new"] == True].drop(columns=["_is_new"], errors="ignore")
        return result.sort_values("timestamp").reset_index(drop=True)

    def calculate_daily_indicators_with_history(self, new_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Calculate all daily indicators including ADX and slopes with historical continuity."""
        if new_df.empty:
            return new_df

        historical_df = self.get_historical_daily_bars_from_db(symbol, EMA_HISTORY_BARS)

        new_df = self.calculate_indicator_with_history(new_df, historical_df, self._calc_atr, "atr")
        new_df = self.calculate_indicator_with_history(new_df, historical_df, self._calc_rsi, "rsi_10")
        new_df = self.calculate_indicator_with_history(new_df, historical_df, self._calc_momentum, "momentum_10")
        new_df = self.calculate_ema_with_history(new_df, historical_df, [8, 20, 39, 50])
        new_df = self.calculate_indicator_with_history(new_df, historical_df, self._calc_adx, "adx")
        new_df = self._calc_slopes(new_df, window=3)

        return new_df

    def calculate_vwap_series(self, df: pd.DataFrame, include_bands: bool = False,
                              multiplier: float = 1) -> pd.DataFrame:
        """Calculate VWAP series. Uses ET date for session grouping but preserves UTC timestamps."""
        if df.empty:
            return df

        df = df.copy().sort_values("timestamp").reset_index(drop=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
        else:
            df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")

        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["pv"] = df["typical_price"] * df["volume"]

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

        df = df.drop(columns=["typical_price", "pv", "cum_pv", "cum_vol_vwap", "_date_et"], errors="ignore")
        return df

    def calculate_rvol_incremental(
            self,
            df: pd.DataFrame,
            symbol: str,
            table_name: str,
            interval_minutes: int
    ) -> pd.DataFrame:
        """Calculate RVOL for incremental data using stored baselines."""
        if df.empty:
            return df

        num_slots = SLOTS_5MIN if interval_minutes == 5 else SLOTS_15MIN

        df = df.copy().sort_values("timestamp").reset_index(drop=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
        else:
            df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")

        df["_session_date"] = df["timestamp"].apply(self._get_session_date_from_utc)
        df["slot_index"] = df["timestamp"].apply(lambda ts: self._compute_slot_index_from_utc(ts, interval_minutes))

        df = df[df["slot_index"].notna()].copy()
        if df.empty:
            return df

        df["slot_index"] = df["slot_index"].astype(int)

        df = df.sort_values(["_session_date", "timestamp"]).reset_index(drop=True)
        df["cum_volume"] = df.groupby("_session_date")["volume"].cumsum()

        baselines = self.get_rvol_baselines_from_db(symbol, table_name, num_slots)

        df["rvol_slot_20"] = np.nan
        df["rvol_slot_baseline_20"] = np.nan
        df["rvol_cum_20"] = np.nan
        df["rvol_cum_baseline_20"] = np.nan
        df["intraday_rvol_20"] = np.nan
        df["avg_daily_volume_20"] = np.nan
        df["pct_of_day_typical"] = np.nan

        for idx, row in df.iterrows():
            slot = row["slot_index"]
            if slot not in baselines:
                continue

            baseline = baselines[slot]

            df.at[idx, "rvol_slot_baseline_20"] = baseline.median_volume
            if baseline.median_volume > 0:
                df.at[idx, "rvol_slot_20"] = round(row["volume"] / baseline.median_volume, 4)

            df.at[idx, "rvol_cum_baseline_20"] = baseline.median_cum_volume
            if baseline.median_cum_volume > 0:
                df.at[idx, "rvol_cum_20"] = round(row["cum_volume"] / baseline.median_cum_volume, 4)

            df.at[idx, "pct_of_day_typical"] = baseline.pct_of_day_typical
            df.at[idx, "avg_daily_volume_20"] = baseline.avg_daily_volume

            if baseline.pct_of_day_typical > 0 and baseline.avg_daily_volume > 0:
                projected_eod = row["cum_volume"] / baseline.pct_of_day_typical
                df.at[idx, "intraday_rvol_20"] = round(projected_eod / baseline.avg_daily_volume, 4)

        df = df.drop(columns=["_session_date"], errors="ignore")
        df = df.sort_values("timestamp").reset_index(drop=True)

        return df

    def calculate_rvol_incremental_first_hour(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Calculate RVOL for first hour 1-minute candles using stored baselines."""
        if df.empty:
            return df

        df = df.copy().sort_values("timestamp").reset_index(drop=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
        else:
            df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")

        df["_session_date"] = df["timestamp"].apply(self._get_session_date_from_utc)
        df["slot_index"] = df["timestamp"].apply(self._compute_first_hour_slot_index)

        df = df[df["slot_index"].notna()].copy()
        if df.empty:
            return df

        df["slot_index"] = df["slot_index"].astype(int)

        df = df.sort_values(["_session_date", "timestamp"]).reset_index(drop=True)
        df["cum_volume"] = df.groupby("_session_date")["volume"].cumsum()

        # Calculate IB metrics per session
        ib_stats = df.groupby("_session_date").agg(
            ib_high=("high", "max"),
            ib_low=("low", "min")
        ).reset_index()
        ib_stats["ib_range"] = ib_stats["ib_high"] - ib_stats["ib_low"]
        df = df.merge(ib_stats, on="_session_date", how="left")

        baselines = self.get_first_hour_rvol_baselines_from_db(symbol)

        df["rvol_slot_20"] = np.nan
        df["rvol_slot_baseline_20"] = np.nan
        df["rvol_cum_20"] = np.nan
        df["rvol_cum_baseline_20"] = np.nan

        for idx, row in df.iterrows():
            slot = row["slot_index"]
            if slot not in baselines:
                continue

            baseline = baselines[slot]

            df.at[idx, "rvol_slot_baseline_20"] = baseline.median_volume
            if baseline.median_volume > 0:
                df.at[idx, "rvol_slot_20"] = round(row["volume"] / baseline.median_volume, 4)

            df.at[idx, "rvol_cum_baseline_20"] = baseline.median_cum_volume
            if baseline.median_cum_volume > 0:
                df.at[idx, "rvol_cum_20"] = round(row["cum_volume"] / baseline.median_cum_volume, 4)

        # Round IB metrics
        df["ib_high"] = df["ib_high"].round(2)
        df["ib_low"] = df["ib_low"].round(2)
        df["ib_range"] = df["ib_range"].round(2)

        df = df.drop(columns=["_session_date"], errors="ignore")
        df = df.sort_values("timestamp").reset_index(drop=True)

        return df

    def calculate_rvol_incremental_5m(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        return self.calculate_rvol_incremental(df, symbol, "candles_5min", 5)

    def calculate_rvol_incremental_15m(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        return self.calculate_rvol_incremental(df, symbol, "candles_15min", 15)

    def calculate_volume_profile_optimized(self, df: pd.DataFrame, price_levels: int = 70) -> VolumeProfileMetrics:
        if df.empty:
            raise ValueError("DataFrame is empty")

        high_price, low_price = df["high"].max(), df["low"].min()
        price_range = high_price - low_price

        if price_range == 0:
            total_vol = int(df["volume"].sum())
            return VolumeProfileMetrics(
                round(float(high_price), 2), round(float(high_price), 2), round(float(high_price), 2),
                total_vol,
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

            df["timestamp"] = pd.to_datetime(df["timestamp"])

            df["is_market_hours"] = df["timestamp"].apply(self.is_market_hours)
            df = df[df["is_market_hours"]].copy()
            df = df.drop(columns=["is_market_hours"], errors="ignore")

            return df
        except Exception as e:
            logging.error(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()

    def fetch_recent_first_hour_data(self, symbol: str, days_back: int = DEFAULT_INCREMENTAL_DAYS) -> pd.DataFrame:
        """Fetch recent first hour 1-minute data from Alpaca."""
        end_date = datetime.now(self.est_tz)
        start_date = datetime.now(self.est_tz) - timedelta(days=days_back)

        try:
            self.api_call_count += 1
            bars = self.client.get_stock_bars(
                StockBarsRequest(
                    symbol_or_symbols=[symbol],
                    timeframe=TimeFrame(1, TimeFrameUnit.Minute),
                    start=start_date.isoformat(),
                    end=end_date.isoformat()
                )
            )
            df = bars.df.reset_index()
            if df.empty:
                return pd.DataFrame()

            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Filter to first hour only (9:30-10:30 AM ET)
            df["is_first_hour"] = df["timestamp"].apply(self.is_first_hour)
            df = df[df["is_first_hour"]].copy()
            df = df.drop(columns=["is_first_hour"], errors="ignore")

            return df
        except Exception as e:
            logging.error(f"Error fetching first hour data for {symbol}: {e}")
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

            df["_timestamp_et"] = df["timestamp"].dt.tz_convert(est)
            df["_time_dec"] = df["_timestamp_et"].dt.hour + df["_timestamp_et"].dt.minute / 60.0
            df = df[(df["_time_dec"] >= 9.5) & (df["_time_dec"] < 16.0)].copy()

            if df.empty:
                return {d.isoformat(): None for d in dates_as_date}, pd.DataFrame()

            df["date"] = df["_timestamp_et"].dt.date
            df = df.drop(columns=["_timestamp_et", "_time_dec"], errors="ignore")

            day_dfs = {}
            for d in dates_as_date:
                day_df = df[df["date"] == d].copy()
                if not day_df.empty:
                    day_df = self.calculate_vwap_series(day_df, include_bands=True)
                day_dfs[d] = day_df

        except Exception as e:
            logging.error(f"Failed to fetch session data for {symbol}: {e}")
            return {d.isoformat(): None for d in dates_as_date}, pd.DataFrame()

        results = {}
        profiles_to_store = []

        for idx, d in enumerate(dates_as_date):
            date_str = d.isoformat()
            day_df = day_dfs.get(d, pd.DataFrame())

            if day_df.empty:
                results[date_str] = None
                continue

            try:
                vp = self.calculate_volume_profile_optimized(day_df)
                session_high, session_low, poc = vp.value_area_high, vp.value_area_low, vp.poc_price
            except:
                session_high, session_low = day_df["high"].max(), day_df["low"].min()
                poc = day_df.loc[day_df["volume"].idxmax(), "close"] if not day_df.empty else None

            results[date_str] = SessionStats(
                day_df["high"].max(), session_high, session_low, poc,
                day_df["vwap_upper"].iloc[-1] if "vwap_upper" in day_df.columns else None,
                day_df["vwap_lower"].iloc[-1] if "vwap_lower" in day_df.columns else None,
                day_df["volume"].sum()
            )

            for lb in [2, 3, 4]:
                start_idx = idx - lb
                if start_idx < 0:
                    continue
                window_dates = dates_as_date[start_idx:idx]
                dfs_to_combine = [day_dfs[wd] for wd in window_dates if not day_dfs.get(wd, pd.DataFrame()).empty]
                if not dfs_to_combine:
                    continue
                comp_df = pd.concat(dfs_to_combine, ignore_index=True)
                if comp_df.empty:
                    continue
                try:
                    vp = self.calculate_volume_profile_optimized(comp_df, 70)
                    sorted_by_vol = sorted(vp.price_levels, key=lambda x: x.volume, reverse=True)
                    hvn = [round(v.price_level, 2) for v in sorted_by_vol[:3]]
                    non_zero = [v for v in vp.price_levels if v.volume > 0]
                    lvn = [round(v.price_level, 2) for v in sorted(non_zero, key=lambda x: x.volume)[:3]]
                    mid = (comp_df["high"].max() + comp_df["low"].min()) / 2
                    width = max(comp_df["high"].max() - comp_df["low"].min(), 1e-6)
                    imbalance = min(100.0, abs(vp.poc_price - mid) / width * 200.0)
                    profiles_to_store.append(CompositeSessionProfile(
                        symbol, d, lb, vp.poc_price, vp.value_area_high, vp.value_area_low,
                        vp.total_volume, hvn, lvn, imbalance
                    ))
                except Exception as e:
                    logging.error(f"Failed composite for {symbol} lb={lb}: {e}")

        if profiles_to_store:
            self._store_composite_session_profiles_batch(profiles_to_store)

        daily_records = []
        for d, day_df in sorted(day_dfs.items()):
            if day_df.empty:
                continue
            day_df = day_df.sort_values("timestamp").reset_index(drop=True)
            rec = {
                "symbol": symbol, "timestamp": pd.Timestamp(d),
                "open": float(day_df["open"].iloc[0]), "high": float(day_df["high"].max()),
                "low": float(day_df["low"].min()), "close": float(day_df["close"].iloc[-1]),
                "volume": int(day_df["volume"].sum()),
            }
            if rec["volume"] > 0:
                tp = (day_df["high"] + day_df["low"] + day_df["close"]) / 3
                rec["vwap"] = round(float((tp * day_df["volume"]).sum() / rec["volume"]), 2)
            else:
                rec["vwap"] = float(rec["close"])
            daily_records.append(rec)

        daily_df = pd.DataFrame(daily_records).sort_values("timestamp").reset_index(
            drop=True) if daily_records else pd.DataFrame()
        return results, daily_df

    def upsert_candles(self, data_dict: Dict[str, pd.DataFrame], table_name: str):
        """Upsert candle data."""
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
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
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

    def upsert_first_hour_candles(self, data_dict: Dict[str, pd.DataFrame]):
        """Upsert first hour 1-minute candle data."""
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
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
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
                f"INSERT INTO candles_1min_first_hour ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))}) ON CONFLICT (symbol, timestamp) DO UPDATE SET {update_clause}",
                [[r.get(c) for c in columns] for r in all_records],
                page_size=DB_BATCH_SIZE
            )
            conn.commit()
            logging.info(f"Upserted {len(all_records)} records into candles_1min_first_hour")
        except Exception as e:
            logging.error(f"Error upserting to candles_1min_first_hour: {e}")
            conn.rollback()
        finally:
            if cur:
                cur.close()
            conn.close()

    def upsert_daily_bars(self, all_bars: Dict[str, List[dict]]):
        """Upsert daily bar data with TrendEngine indicators."""
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
                "poc", "atr", "rsi_10", "momentum_10", "ema_8", "ema_20", "ema_39_of_15min",
                "ema_39", "ema_50", "ema_39_slope", "ema_50_slope",
                "adx", "adx_sma", "adx_slope", "di_plus", "di_minus",
                # IB metrics
                "ib_high", "ib_low", "ib_range", "ib_volume",
                # Outcome tracking
                "ib_break_up", "ib_break_down", "close_vs_ib",
                # Precomputed ratios
                "atr_15min_ratio_10d", "ib_range_ratio_10d",
                # Volume metrics
                "avg_daily_volume_20"
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

    def update_daily_bars_with_ib_metrics(self, symbols: List[str]):
        """
        Update daily_bars with IB metrics from the first 15-min candle of each day.

        IB (Initial Balance) = first 15-min candle:
        - ib_high, ib_low, ib_range, ib_volume
        - ib_break_up: True if day's high > ib_high
        - ib_break_down: True if day's low < ib_low
        - close_vs_ib: 'above' / 'within' / 'below'
        """
        if not symbols:
            return

        conn = self._get_pg_conn()
        cur = None
        try:
            cur = conn.cursor()
            # Get first 15-min candle (slot_index = 0) for each day
            cur.execute(
                """
                WITH first_candle AS (SELECT DISTINCT
                ON (symbol, timestamp :: date)
                    symbol,
                    timestamp :: date as trade_date,
                    high as ib_high,
                    low as ib_low,
                    high - low as ib_range,
                    volume as ib_volume
                FROM candles_15min
                WHERE symbol = ANY (%s)
                  AND slot_index = 0
                ORDER BY symbol, timestamp :: date, timestamp
                    ),
                    day_stats AS (
                SELECT
                    symbol, timestamp :: date as trade_date, MAX (high) as day_high, MIN (low) as day_low, (array_agg(close ORDER BY timestamp DESC))[1] as day_close
                FROM candles_15min
                WHERE symbol = ANY (%s)
                GROUP BY symbol, timestamp :: date
                    )
                UPDATE daily_bars d
                SET ib_high       = fc.ib_high,
                    ib_low        = fc.ib_low,
                    ib_range      = fc.ib_range,
                    ib_volume     = fc.ib_volume,
                    ib_break_up   = (ds.day_high > fc.ib_high),
                    ib_break_down = (ds.day_low < fc.ib_low),
                    close_vs_ib   = CASE
                                        WHEN ds.day_close > fc.ib_high THEN 'above'
                                        WHEN ds.day_close < fc.ib_low THEN 'below'
                                        ELSE 'within'
                        END FROM first_candle fc
                JOIN day_stats ds
                ON fc.symbol = ds.symbol AND fc.trade_date = ds.trade_date
                WHERE d.symbol = fc.symbol
                  AND d.timestamp = fc.trade_date;
                """,
                (symbols, symbols),
            )
            conn.commit()
            logging.info(f"Updated IB metrics in daily_bars for {len(symbols)} symbols")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating IB metrics: {e}")
        finally:
            if cur:
                cur.close()
            conn.close()

    def update_daily_bars_with_ratios(self, symbols: List[str], lookback_days: int = 10):
        """
        Update daily_bars with precomputed ratios:
        - atr_15min_ratio_10d: today's last ATR from 15min / SMA(last 10 days' ATR)
        - ib_range_ratio_10d: today's ib_range / SMA(last 10 days' ib_range)
        """
        if not symbols:
            return

        conn = self._get_pg_conn()
        cur = None
        try:
            cur = conn.cursor()
            # ATR ratio from 15-min candles (using last ATR of each day)
            cur.execute(
                """
                WITH daily_atr AS (SELECT DISTINCT
                ON (symbol, timestamp :: date)
                    symbol,
                    timestamp :: date as trade_date,
                    atr as atr_15min
                FROM candles_15min
                WHERE symbol = ANY (%s)
                  AND atr IS NOT NULL
                ORDER BY symbol, timestamp :: date, timestamp DESC
                    ),
                    atr_with_sma AS (
                SELECT
                    symbol, trade_date, atr_15min, AVG (atr_15min) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN %s PRECEDING AND 1 PRECEDING
                    ) as atr_sma_10d
                FROM daily_atr
                    )
                UPDATE daily_bars d
                SET atr_15min_ratio_10d = ROUND((a.atr_15min / NULLIF(a.atr_sma_10d, 0)):: numeric, 4) FROM atr_with_sma a
                WHERE d.symbol = a.symbol
                  AND d.timestamp = a.trade_date
                  AND a.atr_sma_10d IS NOT NULL;
                """,
                (symbols, lookback_days),
            )

            # IB range ratio
            cur.execute(
                """
                WITH ib_with_sma AS (SELECT symbol,
                    timestamp as trade_date
                   , ib_range
                   , AVG (ib_range) OVER (
                    PARTITION BY symbol
                    ORDER BY timestamp
                    ROWS BETWEEN %s PRECEDING AND 1 PRECEDING
                    ) as ib_range_sma_10d
                FROM daily_bars
                WHERE symbol = ANY (%s)
                  AND ib_range IS NOT NULL
                    )
                UPDATE daily_bars d
                SET ib_range_ratio_10d = ROUND((i.ib_range / NULLIF(i.ib_range_sma_10d, 0)):: numeric, 4) FROM ib_with_sma i
                WHERE d.symbol = i.symbol
                  AND d.timestamp = i.trade_date
                  AND i.ib_range_sma_10d IS NOT NULL;
                """,
                (lookback_days, symbols),
            )

            conn.commit()
            logging.info(f"Updated ATR and IB ratios in daily_bars for {len(symbols)} symbols")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating ratios: {e}")
        finally:
            if cur:
                cur.close()
            conn.close()

    def update_daily_bars_with_avg_volume(self, symbols: List[str]):
        """
        Update daily_bars with avg_daily_volume_20 from candles_15min.

        This provides normalized access to the 20-day average daily volume
        for real-time RVOL calculations.
        """
        if not symbols:
            return

        conn = self._get_pg_conn()
        cur = None
        try:
            cur = conn.cursor()
            # Get the most recent avg_daily_volume_20 for each symbol/date from 15min candles
            cur.execute(
                """
                WITH latest_avg_vol AS (SELECT DISTINCT
                ON (symbol, timestamp :: date)
                    symbol,
                    timestamp :: date as trade_date,
                    avg_daily_volume_20
                FROM candles_15min
                WHERE symbol = ANY (%s)
                  AND avg_daily_volume_20 IS NOT NULL
                ORDER BY symbol, timestamp :: date, timestamp DESC
                    )
                UPDATE daily_bars d
                SET avg_daily_volume_20 = v.avg_daily_volume_20 FROM latest_avg_vol v
                WHERE d.symbol = v.symbol
                  AND d.timestamp = v.trade_date;
                """,
                (symbols,),
            )

            conn.commit()
            logging.info(f"Updated avg_daily_volume_20 in daily_bars for {len(symbols)} symbols")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating avg_daily_volume_20: {e}")
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
            "candles_15min": pd.DataFrame(),
            "candles_1min_first_hour": pd.DataFrame()
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
                        "ema_20": float(row.get("ema_20")) if pd.notna(row.get("ema_20")) else None,
                        "ema_39": float(row.get("ema_39")) if pd.notna(row.get("ema_39")) else None,
                        "ema_50": float(row.get("ema_50")) if pd.notna(row.get("ema_50")) else None,
                        "ema_39_slope": float(row.get("ema_39_slope")) if pd.notna(row.get("ema_39_slope")) else None,
                        "ema_50_slope": float(row.get("ema_50_slope")) if pd.notna(row.get("ema_50_slope")) else None,
                        "adx": float(row.get("adx")) if pd.notna(row.get("adx")) else None,
                        "adx_sma": float(row.get("adx_sma")) if pd.notna(row.get("adx_sma")) else None,
                        "adx_slope": float(row.get("adx_slope")) if pd.notna(row.get("adx_slope")) else None,
                        "di_plus": float(row.get("di_plus")) if pd.notna(row.get("di_plus")) else None,
                        "di_minus": float(row.get("di_minus")) if pd.notna(row.get("di_minus")) else None,
                    })

            # Process 5min data
            data_5min = self.fetch_recent_intraday_data(symbol, TimeFrame(5, TimeFrameUnit.Minute), days_back)
            if not data_5min.empty:
                data_5min = self.calculate_vwap_series(data_5min)
                historical_5min = self.get_historical_candles_from_db(symbol, "candles_5min", EMA_HISTORY_BARS)
                data_5min = self.calculate_ema_with_history(data_5min, historical_5min, [8, 20, 39, 50])
                data_5min = self._calc_intraday_slopes(data_5min, window=3)
                data_5min = self.calculate_intraday_atr_with_history(data_5min, historical_5min,
                                                                     period=12)  # ATR(12) for 5-min
                data_5min = self.calculate_rvol_incremental_5m(data_5min, symbol)

                if data_5min["timestamp"].dt.tz is not None:
                    data_5min["timestamp"] = data_5min["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)

                cols = [
                    "symbol", "timestamp", "open", "high", "low", "close", "volume", "vwap",
                    "ema_8", "ema_20", "ema_39", "ema_50", "ema_39_slope", "ema_50_slope",
                    "slot_index", "cum_volume", "rvol_slot_20", "rvol_slot_baseline_20",
                    "rvol_cum_20", "rvol_cum_baseline_20", "intraday_rvol_20", "avg_daily_volume_20",
                    "pct_of_day_typical",
                    "atr"
                ]
                result["candles_5min"] = data_5min[[c for c in cols if c in data_5min.columns]]

            # Process 15min data
            data_15min = self.fetch_recent_intraday_data(symbol, TimeFrame(15, TimeFrameUnit.Minute), days_back)
            if not data_15min.empty:
                data_15min = self.calculate_vwap_series(data_15min)
                historical_15min = self.get_historical_candles_from_db(symbol, "candles_15min", EMA_HISTORY_BARS)
                data_15min = self.calculate_ema_with_history(data_15min, historical_15min, [8, 20, 39, 50])
                data_15min = self._calc_intraday_slopes(data_15min, window=3)
                data_15min = self.calculate_intraday_atr_with_history(data_15min, historical_15min,
                                                                      period=8)  # ATR(8) for 15-min
                data_15min = self.calculate_rvol_incremental_15m(data_15min, symbol)

                if data_15min["timestamp"].dt.tz is not None:
                    data_15min["timestamp"] = data_15min["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)

                cols = [
                    "symbol", "timestamp", "open", "high", "low", "close", "volume", "vwap",
                    "ema_8", "ema_20", "ema_39", "ema_50", "ema_39_slope", "ema_50_slope",
                    "slot_index", "cum_volume", "rvol_slot_20", "rvol_slot_baseline_20",
                    "rvol_cum_20", "rvol_cum_baseline_20", "intraday_rvol_20", "avg_daily_volume_20",
                    "pct_of_day_typical",
                    "atr"
                ]
                result["candles_15min"] = data_15min[[c for c in cols if c in data_15min.columns]]

            # Process first hour 1-minute data
            data_1min_fh = self.fetch_recent_first_hour_data(symbol, days_back)
            if not data_1min_fh.empty:
                data_1min_fh = self.calculate_vwap_series(data_1min_fh)
                historical_1min_fh = self.get_historical_candles_from_db(symbol, "candles_1min_first_hour",
                                                                         EMA_HISTORY_BARS)
                data_1min_fh = self.calculate_ema_with_history(data_1min_fh, historical_1min_fh, [8, 20, 39, 50])
                data_1min_fh = self._calc_intraday_slopes(data_1min_fh, window=3)
                data_1min_fh = self.calculate_rvol_incremental_first_hour(data_1min_fh, symbol)

                if data_1min_fh["timestamp"].dt.tz is not None:
                    data_1min_fh["timestamp"] = data_1min_fh["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)

                cols = [
                    "symbol", "timestamp", "open", "high", "low", "close", "volume", "vwap",
                    "ema_8", "ema_20", "ema_39", "ema_50", "ema_39_slope", "ema_50_slope",
                    "slot_index", "cum_volume", "rvol_slot_20", "rvol_slot_baseline_20",
                    "rvol_cum_20", "rvol_cum_baseline_20", "ib_high", "ib_low", "ib_range"
                ]
                result["candles_1min_first_hour"] = data_1min_fh[[c for c in cols if c in data_1min_fh.columns]]

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
                        "candles_15min": pd.DataFrame(),
                        "candles_1min_first_hour": pd.DataFrame()
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

            daily_dict, c5_dict, c15_dict, c1_fh_dict = {}, {}, {}, {}
            for symbol, data in results.items():
                if data["daily_bars"]:
                    daily_dict[symbol] = data["daily_bars"]
                if not data["candles_5min"].empty:
                    c5_dict[symbol] = data["candles_5min"]
                if not data["candles_15min"].empty:
                    c15_dict[symbol] = data["candles_15min"]
                if not data["candles_1min_first_hour"].empty:
                    c1_fh_dict[symbol] = data["candles_1min_first_hour"]

            self.upsert_daily_bars(daily_dict)
            self.upsert_candles(c5_dict, "candles_5min")
            self.upsert_candles(c15_dict, "candles_15min")
            self.upsert_first_hour_candles(c1_fh_dict)
            self.update_daily_bars_with_ema39_of_15min(batch)

            # Update IB metrics from first 15-min candle
            self.update_daily_bars_with_ib_metrics(batch)

            # Update ATR and IB ratios
            self.update_daily_bars_with_ratios(batch, lookback_days=10)

            # Update avg_daily_volume_20 from intraday tables
            self.update_daily_bars_with_avg_volume(batch)

            logging.info(f"Batch completed in {time.time() - batch_start:.2f} seconds")

        total_time = time.time() - self.start_time
        logging.info(f"Incremental update completed in {total_time:.2f} seconds")
        logging.info(
            f"Total API calls: {self.api_call_count}, Avg per ticker: {self.api_call_count / len(tickers):.2f}")

    def get_update_summary(self):
        """Print summary of today's data including first hour candles."""
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
                        SELECT 'candles_1min_first_hour', COUNT(*), COUNT(DISTINCT symbol)
                        FROM candles_1min_first_hour
                        WHERE timestamp ::date = CURRENT_DATE
                        UNION ALL
                        SELECT 'daily_bars', COUNT(*), COUNT(DISTINCT symbol)
                        FROM daily_bars
                        WHERE timestamp = CURRENT_DATE
                        """)

            print("\n=== Today's Data Summary ===")
            for row in cur.fetchall():
                print(f"{row[0]}: {row[1]} records, {row[2]} symbols")

            # First hour 1-min quality check
            cur.execute("""
                        SELECT COUNT(*),
                               COUNT(rvol_slot_20),
                               COUNT(rvol_cum_20),
                               COUNT(ib_high),
                               ROUND(AVG(rvol_slot_20)::numeric, 2),
                               ROUND(AVG(ib_range)::numeric, 2)
                        FROM candles_1min_first_hour
                        WHERE timestamp ::date = CURRENT_DATE
                        """)
            row = cur.fetchone()
            if row and row[0] > 0:
                print(f"\nFirst Hour 1-Min Quality (today):")
                print(f"  Total: {row[0]}")
                print(f"  SlotRVOL: {row[1]} ({100 * row[1] / row[0]:.1f}%)")
                print(f"  CumRVOL: {row[2]} ({100 * row[2] / row[0]:.1f}%)")
                print(f"  IB metrics: {row[3]} ({100 * row[3] / row[0]:.1f}%)")
                print(f"  AvgSlotRVOL: {row[4]}, AvgIBRange: {row[5]}")

            # 5min quality
            cur.execute("""
                        SELECT COUNT(*),
                               COUNT(rvol_slot_20),
                               COUNT(intraday_rvol_20),
                               ROUND(AVG(rvol_slot_20)::numeric, 2),
                               ROUND(AVG(intraday_rvol_20)::numeric, 2),
                               COUNT(ema_8),
                               COUNT(ema_39),
                               COUNT(ema_50),
                               COUNT(ema_39_slope),
                               COUNT(ema_50_slope)
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
                print(f"  EMA50: {row[7]} ({100 * row[7] / row[0]:.1f}%)")
                print(f"  EMA39 Slope: {row[8]} ({100 * row[8] / row[0]:.1f}%)")
                print(f"  EMA50 Slope: {row[9]} ({100 * row[9] / row[0]:.1f}%)")

            # 15min quality
            cur.execute("""
                        SELECT COUNT(*),
                               COUNT(rvol_slot_20),
                               COUNT(intraday_rvol_20),
                               ROUND(AVG(rvol_slot_20)::numeric, 2),
                               ROUND(AVG(intraday_rvol_20)::numeric, 2),
                               COUNT(ema_8),
                               COUNT(ema_39),
                               COUNT(ema_50),
                               COUNT(ema_39_slope),
                               COUNT(ema_50_slope)
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
                print(f"  EMA50: {row[7]} ({100 * row[7] / row[0]:.1f}%)")
                print(f"  EMA39 Slope: {row[8]} ({100 * row[8] / row[0]:.1f}%)")
                print(f"  EMA50 Slope: {row[9]} ({100 * row[9] / row[0]:.1f}%)")

            # Daily bars TrendEngine quality
            cur.execute("""
                        SELECT COUNT(*),
                               COUNT(ema_39),
                               COUNT(ema_50),
                               COUNT(ema_39_slope),
                               COUNT(adx),
                               COUNT(adx_slope),
                               COUNT(di_plus),
                               ROUND(AVG(adx)::numeric, 2)
                        FROM daily_bars
                        WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
                        """)
            row = cur.fetchone()

            if row and row[0] > 0:
                print(f"\nTrendEngine Quality (last 7 days daily_bars):")
                print(f"  Total: {row[0]}")
                print(f"  EMA39: {row[1]} ({100 * row[1] / row[0]:.1f}%)")
                print(f"  EMA50: {row[2]} ({100 * row[2] / row[0]:.1f}%)")
                print(f"  EMA39 Slope: {row[3]} ({100 * row[3] / row[0]:.1f}%)")
                print(f"  ADX: {row[4]} ({100 * row[4] / row[0]:.1f}%)")
                print(f"  ADX Slope: {row[5]} ({100 * row[5] / row[0]:.1f}%)")
                print(f"  DI+: {row[6]} ({100 * row[6] / row[0]:.1f}%)")
                print(f"  Avg ADX: {row[7]}")

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