#!/usr/bin/env python3
"""
Alpaca Candlestick Data Collector - Ultra Optimized Version
Fetches 5min and 15min candlestick data and stores in PostgreSQL database.
Optimized for processing large numbers of symbols quickly using parallel processing and batch operations.
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
from queue import Queue
import time
from functools import lru_cache

import psycopg2
from psycopg2.extras import execute_batch
import json

# Load environment variables
load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Postgres DSN: you can also set via env var MARKET_DATA_PG_DSN
PG_DSN = os.getenv(
    "MARKET_DATA_PG_DSN",
    "dbname=testmarket user=postgres password=postgres host=localhost port=5432",
)

TICKERS_FILE = "temp_ticker_list.txt" #"ticker_list.txt"
SPECIAL_TICKERS_FILE = "special_ticker_list.txt"  # not used currently
MAX_BARS = 30000

# Market hours in PST (6:30 AM to 1:00 PM PST)
MARKET_START_PST = 6.5  # 6:30 AM
MARKET_END_PST = 12.99  # 1:00 PM

# Optimization parameters
MAX_WORKERS = 20  # Number of parallel threads for API calls
BATCH_SIZE = 10  # Number of symbols to process in each batch
DB_BATCH_SIZE = 100  # Number of records to insert in each database batch
CACHE_SIZE = 1000  # Size of LRU cache for frequently accessed data

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data_collector_optimized.log"),
        logging.StreamHandler(),
    ],
)


@dataclass
class VolumeProfileLevel:
    """Data class for volume profile levels"""
    price_level: float
    volume: int
    percentage: float
    is_poc: bool = False  # Point of Control
    is_value_area: bool = False


@dataclass
class VolumeProfileMetrics:
    """Data class for volume profile metrics"""
    poc_price: float  # Point of Control
    value_area_high: float  # Value Area High
    value_area_low: float  # Value Area Low
    total_volume: int
    price_levels: List[VolumeProfileLevel]


@dataclass
class SessionStats:
    """Data class for session statistics"""
    highest_high: float
    session_high: float
    session_low: float
    poc: float
    vwap_upper: float
    vwap_lower: float
    session_volume: int


@dataclass
class CompositeSessionProfile:
    """Composite session profile across multiple days"""
    symbol: str
    session_date: date  # as-of date (last day in the window)
    lookback_days: int  # e.g. 2 or 3
    composite_poc: float
    composite_vah: float
    composite_val: float
    total_volume: int
    hvn_levels: List[float]
    lvn_levels: List[float]
    imbalance_score: float


@njit(cache=True)
def _distribute_volume_exact(lows, highs, volumes, bin_lowers, bin_uppers, price_levels):
    """
    Numba-accelerated volume distribution - exact match with original logic.

    Original logic: (price_bins[:-1] <= bar_high) & (price_bins[1:] >= bar_low)
    """
    volume_at_price = np.zeros(price_levels, dtype=np.float64)

    for i in range(len(volumes)):
        if volumes[i] == 0:
            continue

        bar_low = lows[i]
        bar_high = highs[i]
        bar_vol = volumes[i]

        # Count affected bins first (exact same logic as original)
        count = 0
        for j in range(price_levels):
            if bin_lowers[j] <= bar_high and bin_uppers[j] >= bar_low:
                count += 1

        if count > 0:
            vol_per_bin = bar_vol / count
            # Distribute volume to affected bins
            for j in range(price_levels):
                if bin_lowers[j] <= bar_high and bin_uppers[j] >= bar_low:
                    volume_at_price[j] += vol_per_bin

    return volume_at_price


@njit(cache=True)
def _calculate_value_area(volume_at_price, price_levels):
    """Numba-accelerated value area calculation."""
    poc_index = 0
    max_vol = volume_at_price[0]
    for i in range(1, price_levels):
        if volume_at_price[i] > max_vol:
            max_vol = volume_at_price[i]
            poc_index = i

    total_volume = volume_at_price.sum()
    value_area_volume = total_volume * 0.7

    current_volume = volume_at_price[poc_index]
    low_index = poc_index
    high_index = poc_index

    while current_volume < value_area_volume:
        expand_low = low_index > 0
        expand_high = high_index < price_levels - 1

        if not expand_low and not expand_high:
            break

        low_vol = volume_at_price[low_index - 1] if expand_low else 0.0
        high_vol = volume_at_price[high_index + 1] if expand_high else 0.0

        if expand_low and (not expand_high or low_vol >= high_vol):
            low_index -= 1
            current_volume += volume_at_price[low_index]
        elif expand_high:
            high_index += 1
            current_volume += volume_at_price[high_index]

    return poc_index, low_index, high_index, total_volume


class AlpacaDataCollectorOptimized:
    def __init__(self, api_key: str, secret_key: str):
        """Initialize the Alpaca data collector with optimization features."""
        self.client = StockHistoricalDataClient(api_key, secret_key)
        self.pst_tz = pytz.timezone('US/Pacific')
        self.est_tz = pytz.timezone('US/Eastern')  # Alpaca uses EST

        # Thread-safe database connection pool
        self.db_lock = threading.Lock()
        self.db_queue = Queue()

        # Cache for frequently accessed data
        self.session_cache = {}
        self.daily_bars_cache = {}

        # Performance tracking
        self.api_call_count = 0
        self.start_time = None

    # -----------------------------------------------------------------------
    # DB helpers
    # -----------------------------------------------------------------------

    def _get_pg_conn(self):
        """Get a new Postgres connection."""
        return psycopg2.connect(PG_DSN)

    def setup_database(self):
        """Create Postgres tables/indexes if they don't exist."""
        conn = self._get_pg_conn()
        cur = conn.cursor()

        try:
            # candles_5min
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS candles_5min (
                    id          SERIAL PRIMARY KEY,
                    symbol      TEXT NOT NULL,
                    timestamp   TIMESTAMP NOT NULL,
                    open        DOUBLE PRECISION NOT NULL,
                    high        DOUBLE PRECISION NOT NULL,
                    low         DOUBLE PRECISION NOT NULL,
                    close       DOUBLE PRECISION NOT NULL,
                    volume      BIGINT NOT NULL,
                    vwap        DOUBLE PRECISION,
                    ema_8       DOUBLE PRECISION,
                    ema_20      DOUBLE PRECISION,
                    ema_39      DOUBLE PRECISION,
                    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (symbol, timestamp)
                );
                """
            )

            # candles_15min
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS candles_15min (
                    id          SERIAL PRIMARY KEY,
                    symbol      TEXT NOT NULL,
                    timestamp   TIMESTAMP NOT NULL,
                    open        DOUBLE PRECISION NOT NULL,
                    high        DOUBLE PRECISION NOT NULL,
                    low         DOUBLE PRECISION NOT NULL,
                    close       DOUBLE PRECISION NOT NULL,
                    volume      BIGINT NOT NULL,
                    vwap        DOUBLE PRECISION,
                    ema_8       DOUBLE PRECISION,
                    ema_20      DOUBLE PRECISION,
                    ema_39      DOUBLE PRECISION,
                    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (symbol, timestamp)
                );
                """
            )

            # daily_bars
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_bars (
                    id           SERIAL PRIMARY KEY,
                    symbol       TEXT NOT NULL,
                    timestamp    DATE NOT NULL,
                    open         DOUBLE PRECISION NOT NULL,
                    high         DOUBLE PRECISION NOT NULL,
                    low          DOUBLE PRECISION NOT NULL,
                    close        DOUBLE PRECISION NOT NULL,
                    volume       BIGINT NOT NULL,
                    vwap         DOUBLE PRECISION,
                    vwap_upper   DOUBLE PRECISION,
                    vwap_lower   DOUBLE PRECISION,
                    session_high DOUBLE PRECISION,
                    session_low  DOUBLE PRECISION,
                    poc          DOUBLE PRECISION,
                    atr          DOUBLE PRECISION,
                    rsi_10       DOUBLE PRECISION,
                    momentum_10  DOUBLE PRECISION,
                    ema_8        DOUBLE PRECISION,
                    ema_20       DOUBLE PRECISION,
                    ema_39_of_15min    DOUBLE PRECISION,   -- NEW
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (symbol, timestamp)
                );
                """
            )

            # composite_session_profiles
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS composite_session_profiles (
                    id               SERIAL PRIMARY KEY,
                    symbol           TEXT      NOT NULL,
                    session_date     DATE      NOT NULL,
                    lookback_days    SMALLINT  NOT NULL,
                    composite_poc    DOUBLE PRECISION,
                    composite_vah    DOUBLE PRECISION,
                    composite_val    DOUBLE PRECISION,
                    total_volume     BIGINT,
                    hvn_levels       JSONB,
                    lvn_levels       JSONB,
                    imbalance_score  DOUBLE PRECISION,
                    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, session_date, lookback_days)
                );
                """
            )

            # Indexes
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_5min_symbol_timestamp ON candles_5min(symbol, timestamp);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_15min_symbol_timestamp ON candles_15min(symbol, timestamp);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_daily_symbol_timestamp ON daily_bars(symbol, timestamp);"
            )

            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_csp_symbol_date ON composite_session_profiles(symbol, session_date);"
            )

            conn.commit()
            logging.info("Postgres database setup completed")

        except Exception as e:
            conn.rollback()
            logging.error(f"Error during database setup: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    # -----------------------------------------------------------------------
    # Tickers & helper functions
    # -----------------------------------------------------------------------

    def load_tickers(self) -> List[str]:
        """Load tickers from the text file."""
        if not os.path.exists(TICKERS_FILE):
            logging.error(f"Tickers file {TICKERS_FILE} not found")
            return []

        with open(TICKERS_FILE, "r") as f:
            tickers = [line.strip().upper() for line in f if line.strip()]

        logging.info(f"Loaded {len(tickers)} tickers")
        return tickers

    def is_market_hours(self, timestamp: datetime) -> bool:
        """Check if timestamp is within market hours (6:30 AM to 1:00 PM PST)."""
        if timestamp.tzinfo is None:
            timestamp = self.est_tz.localize(timestamp)

        pst_time = timestamp.astimezone(self.pst_tz)
        hour_decimal = pst_time.hour + pst_time.minute / 60.0

        return MARKET_START_PST <= hour_decimal <= MARKET_END_PST

    # -----------------------------------------------------------------------
    # Indicator calculations
    # -----------------------------------------------------------------------

    def calculate_rsi(self, df: pd.DataFrame, period: int = 10) -> pd.DataFrame:
        """Calculate RSI (Relative Strength Index) for given period."""
        if df.empty or len(df) < period + 1:
            df["rsi_10"] = np.nan
            return df

        df = df.copy()
        df = df.sort_values("timestamp").reset_index(drop=True)

        df["price_change"] = df["close"].diff()
        df["gain"] = np.where(df["price_change"] > 0, df["price_change"], 0)
        df["loss"] = np.where(df["price_change"] < 0, -df["price_change"], 0)

        avg_gain = df["gain"].rolling(window=period).mean()
        avg_loss = df["loss"].rolling(window=period).mean()

        rs = avg_gain / avg_loss.replace(0, np.nan)
        df["rsi_10"] = 100 - (100 / (1 + rs))
        df["rsi_10"] = df["rsi_10"].round(2)

        df = df.drop(["price_change", "gain", "loss"], axis=1)

        return df

    def calculate_momentum(self, df: pd.DataFrame, period: int = 10) -> pd.DataFrame:
        """Calculate Momentum indicator."""
        if df.empty or len(df) < period + 1:
            df["momentum_10"] = np.nan
            return df

        df = df.copy()
        df = df.sort_values("timestamp").reset_index(drop=True)

        df["momentum_10"] = (df["close"] / df["close"].shift(period)) * 100
        df["momentum_10"] = df["momentum_10"].round(2)

        return df

    def calculate_vwap_series(
            self, df: pd.DataFrame, include_bands: bool = False, multiplier: float = 1
    ) -> pd.DataFrame:
        """Calculate VWAP for each bar with optional bands - optimized version."""
        if df.empty:
            return df

        df = df.copy()
        df = df.sort_values("timestamp").reset_index(drop=True)

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["pv"] = df["typical_price"] * df["volume"]

        if "date" not in df.columns:
            df["date"] = df["timestamp"].dt.date

        df["cum_pv"] = df.groupby("date")["pv"].cumsum()
        df["cum_volume"] = df.groupby("date")["volume"].cumsum()

        df["vwap"] = 0.0
        mask = df["cum_volume"] > 0
        df.loc[mask, "vwap"] = (df.loc[mask, "cum_pv"] / df.loc[mask, "cum_volume"]).round(2)

        if include_bands:
            df["vwap_upper"] = df["vwap"]
            df["vwap_lower"] = df["vwap"]

            for date_val in df["date"].unique():
                date_mask = df["date"] == date_val
                date_df = df[date_mask]

                if len(date_df) == 0:
                    continue

                vwap_values = date_df["vwap"].values
                typical_prices = date_df["typical_price"].values
                volumes = date_df["volume"].values

                upper_band = []
                lower_band = []

                for i in range(len(date_df)):
                    volume_sum = volumes[: i + 1].sum()
                    if volume_sum > 0:
                        weights = volumes[: i + 1] / volume_sum
                        weighted_mean = vwap_values[i]
                        variance = np.sum(
                            weights * (typical_prices[: i + 1] - weighted_mean) ** 2
                        )
                        std_dev = np.sqrt(variance) if variance > 0 else 0

                        upper_band.append(weighted_mean + multiplier * std_dev)
                        lower_band.append(weighted_mean - multiplier * std_dev)
                    else:
                        upper_band.append(vwap_values[i] if i < len(vwap_values) else 0)
                        lower_band.append(vwap_values[i] if i < len(vwap_values) else 0)

                df.loc[date_mask, "vwap_upper"] = np.round(upper_band, 2)
                df.loc[date_mask, "vwap_lower"] = np.round(lower_band, 2)

        columns_to_drop = ["typical_price", "pv", "cum_pv", "cum_volume"]
        df = df.drop(columns=[c for c in columns_to_drop if c in df.columns], axis=1)

        return df

    def calculate_ema_series(self, df: pd.DataFrame, periods: List[int]) -> pd.DataFrame:
        """Calculate Exponential Moving Averages for given periods."""
        if df.empty:
            return df

        df = df.copy()
        df = df.sort_values("timestamp").reset_index(drop=True)

        for period in periods:
            df[f"ema_{period}"] = (
                df["close"].ewm(span=period, adjust=False).mean().round(3)
            )

        return df

    # -----------------------------------------------------------------------
    # Session stats & volume profile
    # -----------------------------------------------------------------------
    def resample_to_daily_bars(self, day_dfs: Dict[date, pd.DataFrame], symbol: str) -> pd.DataFrame:
        """
        Resample 1-minute market hours data to daily bars.

        For each day:
        - Open: first bar's open
        - High: max of all highs
        - Low: min of all lows
        - Close: last bar's close
        - Volume: sum of all volumes
        - VWAP: volume-weighted average price
        """
        daily_records = []

        for d, day_df in sorted(day_dfs.items()):
            if day_df.empty:
                continue

            # Sort by timestamp to ensure correct open/close
            day_df = day_df.sort_values("timestamp").reset_index(drop=True)

            daily_record = {
                "symbol": symbol,
                "timestamp": pd.Timestamp(d),
                "open": float(day_df["open"].iloc[0]),
                "high": float(day_df["high"].max()),
                "low": float(day_df["low"].min()),
                "close": float(day_df["close"].iloc[-1]),
                "volume": int(day_df["volume"].sum()),
            }

            # Calculate VWAP for the day
            if daily_record["volume"] > 0:
                typical_price = (day_df["high"] + day_df["low"] + day_df["close"]) / 3
                daily_record["vwap"] = round(float((typical_price * day_df["volume"]).sum() / daily_record["volume"]), 2)
            else:
                daily_record["vwap"] = float(daily_record["close"])

            daily_records.append(daily_record)

        if not daily_records:
            return pd.DataFrame()

        df = pd.DataFrame(daily_records)
        df = df.sort_values("timestamp").reset_index(drop=True)

        return df

    def fetch_and_process_session_data(
            self, symbol: str, dates: List[str]
    ) -> Tuple[Dict[str, Optional[SessionStats]], pd.DataFrame]:
        """Fetch and process multiple days of session data at once.

        Now also computes and stores composite session profiles (2, 3, 4 days)
        for each session_date using 1-minute bars.

        IMPORTANT: Composite profiles EXCLUDE the current day.
        For example, for session_date 12/05/2025:
          - 2-day profile uses data from [12/03, 12/04]
          - 3-day profile uses data from [12/02, 12/03, 12/04]
          - 4-day profile uses data from [12/01, 12/02, 12/03, 12/04]
        """
        if not dates:
            return {}

        # Convert input strings (YYYY-MM-DD) -> date objects and sort them
        dates_as_date = sorted([datetime.strptime(d, "%Y-%m-%d").date() for d in dates])
        est = self.est_tz

        # ========== SINGLE API call spanning all dates ==========
        start_date = dates_as_date[0]
        end_date = dates_as_date[-1]

        # Fetch from first day 4:00 AM to last day 8:00 PM to capture everything
        session_start = est.localize(
            datetime.combine(start_date, datetime.min.time()).replace(hour=4, minute=0)
        )
        session_end = est.localize(
            datetime.combine(end_date, datetime.min.time()).replace(hour=20, minute=0)
        )

        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=TimeFrame(1, TimeFrameUnit.Minute),
            start=session_start.isoformat(),
            end=session_end.isoformat(),
        )

        try:
            self.api_call_count += 1
            bars = self.client.get_stock_bars(request)
            df = bars.df.reset_index()

            if df.empty:
                return {d.isoformat(): None for d in dates_as_date}

            if "timestamp" not in df.columns:
                logging.error(f"No timestamp column found for {symbol}")
                return {d.isoformat(): None for d in dates_as_date}

            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # ========== Filter to market hours only (9:30 AM - 3:59 PM EST) ==========
            df["timestamp_est"] = df["timestamp"].dt.tz_convert(est)
            df["time_decimal"] = df["timestamp_est"].dt.hour + df["timestamp_est"].dt.minute / 60.0
            df = df[(df["time_decimal"] >= 9.5) & (df["time_decimal"] < 16.0)].copy()
            df = df.drop(columns=["timestamp_est", "time_decimal"])

            if df.empty:
                return {d.isoformat(): None for d in dates_as_date}

            df["date"] = df["timestamp"].dt.tz_convert(est).dt.date

            # ========== Split into date -> DataFrame mapping ==========
            day_dfs: Dict[date, pd.DataFrame] = {}
            for d in dates_as_date:
                day_df = df[df["date"] == d].copy()
                if not day_df.empty:
                    day_df = self.calculate_vwap_series(day_df, include_bands=True)
                day_dfs[d] = day_df

        except Exception as e:
            logging.error(f"Failed to fetch session data for {symbol}: {e}")
            return {d.isoformat(): None for d in dates_as_date}

        results: Dict[str, Optional[SessionStats]] = {}
        profiles_to_store: List[CompositeSessionProfile] = []

        # ========== Helper to build composite profile ==========
        def _build_composite(date_idx: int, lookback_days: int) -> Optional[CompositeSessionProfile]:
            """Build composite profile by combining DataFrames based on date index and lookback.

            IMPORTANT: Excludes the current day (date_idx) and uses the previous N days.
            For example, if date_idx points to 12/05 and lookback_days=2:
              - Uses data from [12/03, 12/04] (excludes 12/05)
              - Stores session_date as 12/05 (the "as-of" date)
            """
            # Exclude current day: use dates from (date_idx - lookback_days) to (date_idx - 1)
            end_idx = date_idx  # exclusive - current day not included
            start_idx = date_idx - lookback_days
            if start_idx < 0:
                return None

            window_dates = dates_as_date[start_idx:end_idx]  # excludes date_idx
            dfs_to_combine = [day_dfs[d] for d in window_dates if not day_dfs[d].empty]
            if not dfs_to_combine:
                return None

            comp_df = pd.concat(dfs_to_combine, ignore_index=True)
            if comp_df.empty:
                return None

            try:
                vp = self.calculate_volume_profile_optimized(comp_df, price_levels=70)
                vols = vp.price_levels
                if not vols:
                    return None

                sorted_by_vol = sorted(vols, key=lambda x: x.volume, reverse=True)
                hvn_levels = [round(v.price_level, 2) for v in sorted_by_vol[:3]]

                non_zero = [v for v in vols if v.volume > 0]
                sorted_low = sorted(non_zero, key=lambda x: x.volume)
                lvn_levels = [round(v.price_level, 2) for v in sorted_low[:3]]

                highs = comp_df["high"].max()
                lows = comp_df["low"].min()
                mid_price = (highs + lows) / 2.0
                width = max(highs - lows, 1e-6)
                asym = abs(vp.poc_price - mid_price) / width
                imbalance_score = min(100.0, asym * 200.0)

                return CompositeSessionProfile(
                    symbol=symbol,
                    session_date=dates_as_date[date_idx],  # Store as-of date (current day)
                    lookback_days=lookback_days,
                    composite_poc=vp.poc_price,
                    composite_vah=vp.value_area_high,
                    composite_val=vp.value_area_low,
                    total_volume=vp.total_volume,
                    hvn_levels=hvn_levels,
                    lvn_levels=lvn_levels,
                    imbalance_score=imbalance_score,
                )
            except Exception as e:
                logging.error(
                    f"Failed composite profile for {symbol} lb={lookback_days} "
                    f"on {dates_as_date[date_idx]}: {e}"
                )
                return None

        # ========== Main loop: iterate by index over dates_as_date ==========
        for idx, d in enumerate(dates_as_date):
            date_str = d.isoformat()
            day_df = day_dfs[d]

            if day_df.empty:
                results[date_str] = None
                continue

            highest_high = day_df["high"].max()
            total_volume = day_df["volume"].sum()

            try:
                volume_profile_metrics = self.calculate_volume_profile_optimized(day_df)
                session_high = volume_profile_metrics.value_area_high
                session_low = volume_profile_metrics.value_area_low
                poc = volume_profile_metrics.poc_price
            except Exception as e:
                logging.warning(
                    f"Could not calculate volume profile for {symbol} on {date_str}: {e}"
                )
                session_high = day_df["high"].max()
                session_low = day_df["low"].min()
                poc = (
                    day_df.loc[day_df["volume"].idxmax(), "close"]
                    if not day_df.empty
                    else None
                )

            vwap_upper = (
                day_df["vwap_upper"].iloc[-1]
                if "vwap_upper" in day_df.columns and len(day_df) > 0
                else None
            )
            vwap_lower = (
                day_df["vwap_lower"].iloc[-1]
                if "vwap_lower" in day_df.columns and len(day_df) > 0
                else None
            )

            results[date_str] = SessionStats(
                highest_high=highest_high,
                session_high=session_high,
                session_low=session_low,
                poc=poc,
                vwap_upper=vwap_upper,
                vwap_lower=vwap_lower,
                session_volume=total_volume,
            )

            # Build composite profiles for lookbacks 2, 3, 4 (excluding current day)
            # Note: lookback=1 would be redundant since it's just yesterday's single-day profile
            for lb in [2, 3, 4]:
                profile = _build_composite(idx, lb)
                if profile:
                    profiles_to_store.append(profile)

        # Single batch insert for all profiles
        if profiles_to_store:
            self._store_composite_session_profiles_batch(profiles_to_store)

        # ========== Resample to daily bars ==========
        daily_bars_df = self.resample_to_daily_bars(day_dfs, symbol)

        return results, daily_bars_df

    def _store_composite_session_profiles_batch(self, profiles: List[CompositeSessionProfile]):
        """Batch insert all composite profiles in ONE transaction."""
        if not profiles:
            return

        conn = self._get_pg_conn()
        cur = conn.cursor()

        try:
            records = []
            for p in profiles:
                records.append((
                    p.symbol,
                    p.session_date,
                    int(p.lookback_days),
                    float(p.composite_poc) if p.composite_poc is not None else None,
                    float(p.composite_vah) if p.composite_vah is not None else None,
                    float(p.composite_val) if p.composite_val is not None else None,
                    int(p.total_volume) if p.total_volume is not None else None,
                    json.dumps([float(x) for x in (p.hvn_levels or [])]),
                    json.dumps([float(x) for x in (p.lvn_levels or [])]),
                    float(p.imbalance_score) if p.imbalance_score is not None else None,
                ))

            execute_batch(
                cur,
                """
                INSERT INTO composite_session_profiles
                (symbol, session_date, lookback_days, composite_poc, composite_vah,
                 composite_val, total_volume, hvn_levels, lvn_levels, imbalance_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (symbol, session_date, lookback_days) DO
                UPDATE SET
                    composite_poc = EXCLUDED.composite_poc,
                    composite_vah = EXCLUDED.composite_vah,
                    composite_val = EXCLUDED.composite_val,
                    total_volume = EXCLUDED.total_volume,
                    hvn_levels = EXCLUDED.hvn_levels,
                    lvn_levels = EXCLUDED.lvn_levels,
                    imbalance_score = EXCLUDED.imbalance_score
                """,
                records,
                page_size=1000
            )

            conn.commit()
            logging.debug(
                f"Batch stored {len(profiles)} composite profiles for {profiles[0].symbol if profiles else 'N/A'}")

        except Exception as e:
            conn.rollback()
            logging.error(f"Error batch storing composite profiles: {e}")
        finally:
            cur.close()
            conn.close()

    def calculate_volume_profile_optimized(
            self, df: pd.DataFrame, price_levels: int = 70
    ) -> VolumeProfileMetrics:
        """
        Numba-accelerated volume profile calculation - exact match with original logic.
        """
        if df.empty:
            raise ValueError("DataFrame is empty")

        high_price = df["high"].max()
        low_price = df["low"].min()
        price_range = high_price - low_price

        # Handle zero range case
        if price_range == 0:
            total_volume = int(df["volume"].sum())
            single_price = float(high_price)
            profile_levels: List[VolumeProfileLevel] = []
            if total_volume > 0:
                profile_levels.append(
                    VolumeProfileLevel(
                        price_level=single_price,
                        volume=total_volume,
                        percentage=100.0,
                        is_poc=True,
                        is_value_area=True,
                    )
                )
            return VolumeProfileMetrics(
                poc_price=round(single_price, 2),
                value_area_high=round(single_price, 2),
                value_area_low=round(single_price, 2),
                total_volume=total_volume,
                price_levels=profile_levels,
            )

        # Extract numpy arrays
        lows = df["low"].values.astype(np.float64)
        highs = df["high"].values.astype(np.float64)
        volumes = df["volume"].values.astype(np.float64)

        # Build price bins
        price_bins = np.linspace(low_price, high_price, price_levels + 1)
        bin_lowers = price_bins[:-1].astype(np.float64)
        bin_uppers = price_bins[1:].astype(np.float64)

        # Numba-accelerated volume distribution
        volume_at_price = _distribute_volume_exact(
            lows, highs, volumes, bin_lowers, bin_uppers, price_levels
        )

        # Numba-accelerated value area calculation
        poc_index, low_index, high_index, total_volume = _calculate_value_area(
            volume_at_price, price_levels
        )

        poc_price = (price_bins[poc_index] + price_bins[poc_index + 1]) / 2
        value_area_high = (price_bins[high_index] + price_bins[high_index + 1]) / 2
        value_area_low = (price_bins[low_index] + price_bins[low_index + 1]) / 2

        # Build profile levels (this part is fast enough in Python)
        non_zero_indices = np.where(volume_at_price > 0)[0]

        profile_levels: List[VolumeProfileLevel] = []
        for i in non_zero_indices:
            price_level = (price_bins[i] + price_bins[i + 1]) / 2
            profile_levels.append(
                VolumeProfileLevel(
                    price_level=float(price_level),
                    volume=int(volume_at_price[i]),
                    percentage=(float(volume_at_price[i]) / total_volume * 100) if total_volume > 0 else 0,
                    is_poc=(i == poc_index),
                    is_value_area=(low_index <= i <= high_index),
                )
            )

        return VolumeProfileMetrics(
            poc_price=round(float(poc_price), 2),
            value_area_high=round(float(value_area_high), 2),
            value_area_low=round(float(value_area_low), 2),
            total_volume=int(total_volume),
            price_levels=profile_levels,
        )

    # -----------------------------------------------------------------------
    # Composite profile helpers (NEW)
    # -----------------------------------------------------------------------

    def calculate_composite_profile_from_candles(
            self, df: pd.DataFrame, price_levels: int = 70
    ) -> VolumeProfileMetrics:
        """
        Build a multi-day composite volume profile using the existing
        optimized volume profile logic.
        """
        return self.calculate_volume_profile_optimized(df, price_levels=price_levels)

    def _store_composite_session_profile(
            self,
            profile: CompositeSessionProfile,
    ):
        """Upsert a CompositeSessionProfile into Postgres."""
        conn = self._get_pg_conn()
        cur = conn.cursor()
        try:
            # Make absolutely sure we don't pass any numpy types to JSON/psycopg
            hvn_levels = [float(x) for x in (profile.hvn_levels or [])]
            lvn_levels = [float(x) for x in (profile.lvn_levels or [])]

            total_volume = int(profile.total_volume) if profile.total_volume is not None else None
            imbalance_score = float(profile.imbalance_score) if profile.imbalance_score is not None else None
            composite_poc = float(profile.composite_poc) if profile.composite_poc is not None else None
            composite_vah = float(profile.composite_vah) if profile.composite_vah is not None else None
            composite_val = float(profile.composite_val) if profile.composite_val is not None else None

            cur.execute(
                """
                INSERT INTO public.composite_session_profiles (symbol,
                                                               session_date,
                                                               lookback_days,
                                                               composite_poc,
                                                               composite_vah,
                                                               composite_val,
                                                               total_volume,
                                                               hvn_levels,
                                                               lvn_levels,
                                                               imbalance_score)
                VALUES (%(symbol)s,
                        %(session_date)s,
                        %(lookback_days)s,
                        %(composite_poc)s,
                        %(composite_vah)s,
                        %(composite_val)s,
                        %(total_volume)s,
                        %(hvn_levels)s,
                        %(lvn_levels)s,
                        %(imbalance_score)s) ON CONFLICT (symbol, session_date, lookback_days)
                DO
                UPDATE SET
                    composite_poc = EXCLUDED.composite_poc,
                    composite_vah = EXCLUDED.composite_vah,
                    composite_val = EXCLUDED.composite_val,
                    total_volume = EXCLUDED.total_volume,
                    hvn_levels = EXCLUDED.hvn_levels,
                    lvn_levels = EXCLUDED.lvn_levels,
                    imbalance_score = EXCLUDED.imbalance_score;
                """,
                {
                    "symbol": profile.symbol,
                    "session_date": profile.session_date,
                    "lookback_days": int(profile.lookback_days),
                    "composite_poc": composite_poc,
                    "composite_vah": composite_vah,
                    "composite_val": composite_val,
                    "total_volume": total_volume,
                    "hvn_levels": json.dumps(hvn_levels),
                    "lvn_levels": json.dumps(lvn_levels),
                    "imbalance_score": imbalance_score,
                },
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(
                f"Error storing composite profile for {profile.symbol}, "
                f"session_date={profile.session_date}, lb={profile.lookback_days}: {e}"
            )
            raise
        finally:
            cur.close()
            conn.close()

    # -----------------------------------------------------------------------
    # Symbol processing / indicators
    # -----------------------------------------------------------------------

    def process_symbol_batch(self, symbols: List[str], days_back: int = 180) -> Dict[str, List[dict]]:
        """Process a batch of symbols in parallel."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        all_results: Dict[str, List[dict]] = {}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_symbol = {
                executor.submit(self.process_single_symbol, symbol, start_date, end_date): symbol
                for symbol in symbols
            }

            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    all_results[symbol] = result
                    logging.info(f"Processed {symbol}")
                except Exception as e:
                    logging.error(f"Error processing {symbol}: {e}")
                    all_results[symbol] = []

        return all_results

    def process_single_symbol(
            self, symbol: str, start_date: datetime, end_date: datetime
    ) -> List[dict]:
        """Process a single symbol - fetch daily bars and session stats."""
        try:
            request = StockBarsRequest(
                symbol_or_symbols=[symbol],
                timeframe=TimeFrame(1, TimeFrameUnit.Day),
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                feed="iex",
            )

            self.api_call_count += 1
            bars = self.client.get_stock_bars(request)
            df = bars.df.reset_index()

            if df.empty:
                return []

            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.sort_values("timestamp").reset_index(drop=True)

            dates_to_fetch = [row["timestamp"].date().isoformat() for _, row in df.iterrows()]
            session_stats, daily_df = self.fetch_and_process_session_data(symbol, dates_to_fetch)

            if daily_df.empty:
                return []

                # Calculate daily indicators on resampled data
            daily_df = self.calculate_daily_indicators_optimized(daily_df)

            result: List[dict] = []
            for _, row in daily_df.iterrows():
                date_str = row["timestamp"].date().isoformat()
                stats = session_stats.get(date_str)

                bar_data = {
                    "symbol": row["symbol"],
                    "timestamp": date_str,
                    "open": float(row["open"]) if pd.notna(row["open"]) else None,
                    "high": float(stats.highest_high)
                    if stats and stats.highest_high
                    else float(row["high"])
                    if pd.notna(row["high"])
                    else None,
                    "low": float(row["low"]) if pd.notna(row["low"]) else None,
                    "close": float(row["close"]) if pd.notna(row["close"]) else None,
                    "volume": int(stats.session_volume) if stats and stats.session_volume else 0,
                    "vwap": float(row.get("vwap")) if pd.notna(row.get("vwap")) else None,
                    "vwap_upper": float(stats.vwap_upper) if stats and stats.vwap_upper else None,
                    "vwap_lower": float(stats.vwap_lower) if stats and stats.vwap_lower else None,
                    "session_high": float(stats.session_high) if stats and stats.session_high else None,
                    "session_low": float(stats.session_low) if stats and stats.session_low else None,
                    "poc": float(stats.poc) if stats and stats.poc else None,
                    "atr": float(row.get("atr")) if pd.notna(row.get("atr")) else None,
                    "rsi_10": float(row.get("rsi_10")) if pd.notna(row.get("rsi_10")) else None,
                    "momentum_10": float(row.get("momentum_10")) if pd.notna(row.get("momentum_10")) else None,
                    "ema_8": float(row.get("ema_8")) if pd.notna(row.get("ema_8")) else None,
                    "ema_20": float(row.get("ema_20")) if pd.notna(row.get("ema_20")) else None,
                }
                result.append(bar_data)

            return result

        except Exception as e:
            logging.error(f"Error processing {symbol}: {e}")
            return []

    def calculate_daily_indicators_optimized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate all daily indicators using vectorized operations."""
        if df.empty:
            return df

        df["high_low"] = df["high"] - df["low"]
        df["high_pc"] = abs(df["high"] - df["close"].shift(1))
        df["low_pc"] = abs(df["low"] - df["close"].shift(1))
        df["true_range"] = df[["high_low", "high_pc", "low_pc"]].max(axis=1)

        df["atr"] = df["true_range"].ewm(span=10, adjust=False).mean()

        df = self.calculate_rsi(df, period=10)
        df = self.calculate_momentum(df, period=10)
        df = self.calculate_ema_series(df, [8, 20])

        df = df.drop(["high_low", "high_pc", "low_pc", "true_range"], axis=1, errors="ignore")

        return df

    # -----------------------------------------------------------------------
    # Intraday data fetch
    # -----------------------------------------------------------------------

    def get_market_hours_data_batch(
            self, symbols: List[str], timeframe: TimeFrame, days_back: int = 360
    ) -> Dict[str, pd.DataFrame]:
        """Fetch market hours data for multiple symbols in parallel."""
        results: Dict[str, pd.DataFrame] = {}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_symbol = {
                executor.submit(
                    self.get_market_hours_data_single, symbol, timeframe, days_back
                ): symbol
                for symbol in symbols
            }

            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    results[symbol] = result
                except Exception as e:
                    logging.error(f"Error fetching market hours data for {symbol}: {e}")
                    results[symbol] = pd.DataFrame()

        return results

    def get_market_hours_data_single(
            self, symbol: str, timeframe: TimeFrame, days_back: int = 10
    ) -> pd.DataFrame:
        """Fetch market hours data for a single symbol."""
        end_date = datetime.now(self.est_tz)
        start_date = end_date - timedelta(days=days_back)

        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=timeframe,
            start=start_date.isoformat(),
            end=end_date.isoformat(),
        )

        try:
            self.api_call_count += 1
            bars = self.client.get_stock_bars(request)
            df = bars.df.reset_index()

            if df.empty:
                return pd.DataFrame()

            df["is_market_hours"] = df["timestamp"].apply(self.is_market_hours)
            market_hours_df = df[df["is_market_hours"]].copy()

            market_hours_df = market_hours_df.tail(MAX_BARS)

            market_hours_df = self.calculate_vwap_series(market_hours_df)
            market_hours_df = self.calculate_ema_series(market_hours_df, [8, 20, 39])

            available_columns = [
                "symbol",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "vwap",
                "ema_8",
                "ema_20",
                "ema_39",
            ]
            market_hours_df = market_hours_df[available_columns]

            return market_hours_df

        except Exception as e:
            logging.error(f"Error fetching data for {symbol}: {str(e)}")
            return pd.DataFrame()

    # -----------------------------------------------------------------------
    # Storage: Postgres
    # -----------------------------------------------------------------------

    def batch_store_data(self, data_dict: Dict[str, pd.DataFrame], table_name: str):
        """Store multiple dataframes in Postgres using batch operations."""
        if not data_dict:
            return

        conn = self._get_pg_conn()
        cur = conn.cursor()

        try:
            all_records: List[dict] = []
            symbols_to_delete = set()

            for symbol, df in data_dict.items():
                if not df.empty:
                    symbols_to_delete.add(symbol)
                    df = df.copy()
                    if 'timestamp' in df.columns:
                        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    records = df.to_dict('records')
                    all_records.extend(records)

            # Optional: delete existing records for these symbols (keeps table smaller)
            if symbols_to_delete:
                placeholders = ",".join(["%s"] * len(symbols_to_delete))
                cur.execute(
                    f"DELETE FROM {table_name} WHERE symbol IN ({placeholders})",
                    list(symbols_to_delete),
                )

            if all_records:
                columns = list(all_records[0].keys())
                col_list = ",".join(columns)
                placeholders = ",".join(["%s"] * len(columns))

                insert_sql = f"""
                    INSERT INTO {table_name} ({col_list})
                    VALUES ({placeholders})
                    ON CONFLICT (symbol, timestamp) DO UPDATE SET
                    {", ".join([f"{col}=EXCLUDED.{col}" for col in columns if col not in ("symbol", "timestamp")])}
                """

                batched = [
                    [record.get(col) for col in columns] for record in all_records
                ]

                for i in range(0, len(batched), DB_BATCH_SIZE):
                    execute_batch(
                        cur,
                        insert_sql,
                        batched[i: i + DB_BATCH_SIZE],
                        page_size=DB_BATCH_SIZE,
                    )

            conn.commit()
            logging.info(f"Batch stored {len(all_records)} records in {table_name}")

        except Exception as e:
            logging.error(f"Error in batch store ({table_name}): {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def batch_store_daily_bars(self, all_bars: Dict[str, List[dict]]):
        """Store all daily bars in Postgres using batch operations."""
        if not all_bars:
            return

        conn = self._get_pg_conn()
        cur = conn.cursor()

        try:
            all_records: List[dict] = []
            for symbol, bars in all_bars.items():
                all_records.extend(bars)

            if not all_records:
                return

            symbols = list(all_bars.keys())
            placeholders = ",".join(["%s"] * len(symbols))
            cur.execute(
                f"DELETE FROM daily_bars WHERE symbol IN ({placeholders})", symbols
            )

            columns = [
                "symbol",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "vwap",
                "vwap_upper",
                "vwap_lower",
                "session_high",
                "session_low",
                "poc",
                "atr",
                "rsi_10",
                "momentum_10",
                "ema_8",
                "ema_20",
                "ema_39_of_15min",
            ]
            col_list = ",".join(columns)
            placeholders = ",".join(["%s"] * len(columns))

            insert_sql = f"""
                INSERT INTO daily_bars
                ({col_list})
                VALUES ({placeholders})
                ON CONFLICT (symbol, timestamp) DO UPDATE SET
                {", ".join([f"{col}=EXCLUDED.{col}" for col in columns if col not in ("symbol", "timestamp")])}
            """

            batched = [
                [
                    bar["symbol"],
                    bar["timestamp"],
                    bar["open"],
                    bar["high"],
                    bar["low"],
                    bar["close"],
                    bar["volume"],
                    bar.get("vwap"),
                    bar.get("vwap_upper"),
                    bar.get("vwap_lower"),
                    bar.get("session_high"),
                    bar.get("session_low"),
                    bar.get("poc"),
                    bar.get("atr"),
                    bar.get("rsi_10"),
                    bar.get("momentum_10"),
                    bar.get("ema_8"),
                    bar.get("ema_20"),
                    bar.get("ema_39_of_15min"),
                ]
                for bar in all_records
            ]

            for i in range(0, len(batched), DB_BATCH_SIZE):
                execute_batch(
                    cur,
                    insert_sql,
                    batched[i: i + DB_BATCH_SIZE],
                    page_size=DB_BATCH_SIZE,
                )

            conn.commit()
            logging.info(f"Batch stored {len(all_records)} daily bars")

        except Exception as e:
            logging.error(f"Error storing daily bars: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def update_daily_bars_with_ema39_of_15min(self, symbols: List[str]):
        """
        For each symbol/date, set daily_bars.ema_39_of_15min to the last
        15-minute ema_39 value of that day from candles_15min.
        """
        if not symbols:
            return

        conn = self._get_pg_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                WITH last_ema AS (SELECT DISTINCT
                ON (symbol, trade_date)
                    symbol,
                    trade_date,
                    ema_39
                FROM (
                    SELECT
                    symbol, timestamp :: date AS trade_date, ema_39, timestamp
                    FROM candles_15min
                    WHERE symbol = ANY (%s)
                    AND ema_39 IS NOT NULL
                    ) t
                ORDER BY symbol, trade_date, timestamp DESC
                    )
                UPDATE daily_bars d
                SET ema_39_of_15min = l.ema_39 FROM last_ema l
                WHERE d.symbol = l.symbol
                  AND d.timestamp = l.trade_date;
                """,
                (symbols,),
            )
            conn.commit()
            logging.info(
                f"Updated ema_39_of_15min in daily_bars for {len(symbols)} symbols"
            )
        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating ema_39_of_15min: {e}")
        finally:
            cur.close()
            conn.close()

    # -----------------------------------------------------------------------
    # Orchestration & summary
    # -----------------------------------------------------------------------

    def collect_all_data_optimized(self, incremental: bool = False):
        """Main optimized method to collect data for all tickers."""
        self.start_time = time.time()
        logging.info("Starting optimized data collection process")

        self.setup_database()

        tickers = self.load_tickers()
        if not tickers:
            logging.error("No tickers to process")
            return

        logging.info(f"Processing {len(tickers)} tickers in batches of {BATCH_SIZE}")

        for i in range(0, len(tickers), BATCH_SIZE):
            batch = tickers[i: i + BATCH_SIZE]
            batch_start_time = time.time()

            logging.info(
                f"Processing batch {i // BATCH_SIZE + 1}/{(len(tickers) - 1) // BATCH_SIZE + 1}: {batch}"
            )

            days_back = 360 if not incremental else 1
            daily_bars_batch = self.process_symbol_batch(batch, days_back)

            # Store daily bars (session stats included)
            self.batch_store_daily_bars(daily_bars_batch)

            # Fetch & store 5min candles (used for composite profile)
            data_5min = self.get_market_hours_data_batch(
                batch, TimeFrame(5, TimeFrameUnit.Minute)
            )
            self.batch_store_data(data_5min, "candles_5min")

            # Fetch & store 15min candles
            data_15min = self.get_market_hours_data_batch(
                batch, TimeFrame(15, TimeFrameUnit.Minute)
            )
            self.batch_store_data(data_15min, "candles_15min")

            # Now that candles_15min are in DB, backfill daily_bars.ema_39_of_15min
            self.update_daily_bars_with_ema39_of_15min(batch)

            batch_time = time.time() - batch_start_time
            logging.info(f"Batch completed in {batch_time:.2f} seconds")

        total_time = time.time() - self.start_time
        logging.info(f"Data collection completed in {total_time:.2f} seconds")
        logging.info(f"Total API calls: {self.api_call_count}")
        logging.info(f"Average time per ticker: {total_time / len(tickers):.2f} seconds")
        logging.info(
            f"API calls per ticker: {self.api_call_count / len(tickers):.2f}"
        )

    def get_data_summary(self):
        """Print summary of stored data from Postgres."""
        conn = self._get_pg_conn()
        cur = conn.cursor()

        try:
            tables = ["candles_5min", "candles_15min", "daily_bars", "composite_session_profiles"]

            for table in tables:
                cur.execute(
                    f"""
                    SELECT 
                        COUNT(DISTINCT symbol) AS symbol_count,
                        COUNT(*) AS total_records,
                        MIN(session_date) AS earliest,
                        MAX(session_date) AS latest
                    FROM {table}
                    """
                    if table == "composite_session_profiles"
                    else f"""
                    SELECT 
                        COUNT(DISTINCT symbol) AS symbol_count,
                        COUNT(*) AS total_records,
                        MIN(timestamp) AS earliest,
                        MAX(timestamp) AS latest
                    FROM {table}
                    """
                )
                symbol_count, total_records, earliest, latest = cur.fetchone()

                print(f"\n{table} Summary:")
                print(f"  Symbols: {symbol_count}")
                print(f"  Total Records: {total_records}")
                print(f"  Date Range: {earliest} to {latest}")

        finally:
            cur.close()
            conn.close()


def main():
    """Main function to run the optimized data collector."""
    API_KEY = os.getenv("APCA_API_KEY_ID")
    SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

    if not API_KEY or not SECRET_KEY:
        print(
            "Error: Please set APCA_API_KEY_ID and APCA_API_SECRET_KEY environment variables"
        )
        return

    try:
        collector = AlpacaDataCollectorOptimized(API_KEY, SECRET_KEY)

        start_time = time.time()
        collector.collect_all_data_optimized(incremental=False)

        collector.get_data_summary()

        total_time = time.time() - start_time
        print(
            f"\n✅ Total execution time: {total_time:.2f} seconds ({total_time / 60:.2f} minutes)"
        )

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise


if __name__ == "__main__":
    main()