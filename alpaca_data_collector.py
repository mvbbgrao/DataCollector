#!/usr/bin/env python3
"""
Alpaca Candlestick Data Collector - Ultra Optimized Version
Fetches 5min, 15min, and 1min (first hour) candlestick data and stores in PostgreSQL database.

NEW: First Hour 1-Minute Candles (candles_1min_first_hour)
- Captures 9:30-10:30 AM ET (60 one-minute bars)
- slot_index: 0-59 mapping to each minute of the first hour
- Includes RVOL calculations and EMA indicators
- Critical for Initial Balance (IB) analysis and early session momentum

RVOL Implementation (5-minute and 15-minute candles):

5-minute candles:
- slot_index: 0-77 mapping to 5-minute slots from 09:30-16:00 ET (78 slots)
- cum_volume: cumulative volume within session

15-minute candles:
- slot_index: 0-25 mapping to 15-minute slots from 09:30-16:00 ET (26 slots)
- cum_volume: cumulative volume within session

1-minute first hour candles:
- slot_index: 0-59 mapping to 1-minute slots from 09:30-10:30 ET (60 slots)
- cum_volume: cumulative volume within first hour

Slot RVOL:
- rvol_slot_20: current slot volume / baseline (median of same slot over prior 20 sessions)
- rvol_slot_baseline_20: the baseline value used

Cumulative RVOL:
- rvol_cum_20: current cumulative volume / baseline cumulative at same slot
- rvol_cum_baseline_20: the baseline value used

All baselines use MEDIAN (not mean) to reduce sensitivity to earnings/news spikes.
Baselines exclude current session (shift by 1).

Daily Bars Indicators:
- ema_8, ema_20, ema_39, ema_50: Exponential Moving Averages
- ema_39_slope, ema_50_slope: 3-day slope of EMAs
- adx: Average Directional Index (14-period, Wilder smoothing)
- adx_sma: SMA of ADX (5-period) for trend strength smoothing
- adx_slope: 3-day slope of ADX
- di_plus, di_minus: Directional Indicators (+DI/-DI)

Intraday Candles (5min/15min/1min) Indicators:
- ema_8, ema_20, ema_39, ema_50: Exponential Moving Averages
- ema_39_slope, ema_50_slope: 3-bar slope of EMAs

NOTE: All timestamps are stored in UTC format. Slot index calculations are done
      internally using ET conversion but timestamps remain in UTC.
"""

from alpaca.data import TimeFrameUnit, Adjustment
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

import psycopg2
from psycopg2.extras import execute_batch
import json

# Load environment variables
load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PG_DSN = os.getenv(
    "MARKET_DATA_PG_DSN",
    "dbname=testmarket user=postgres password=postgres host=localhost port=5432",
)

TICKERS_FILE = "ticker_list.txt"
SPECIAL_TICKERS_FILE = "special_ticker_list.txt"  # not used currently
MAX_BARS = 30000

# Market hours in PST (6:30 AM to 1:00 PM PST = 9:30 AM to 4:00 PM ET)
MARKET_START_PST = 6.5  # 6:30 AM PST
MARKET_END_PST = 12.99  # 1:00 PM PST

# First hour window (ET)
FIRST_HOUR_START_MINUTES = 9 * 60 + 30  # 9:30 AM ET = 570 minutes
FIRST_HOUR_END_MINUTES = 10 * 60 + 30  # 10:30 AM ET = 630 minutes

# Optimization parameters
MAX_WORKERS = 20
BATCH_SIZE = 10
DB_BATCH_SIZE = 100

# RVOL configuration
RVOL_LOOKBACK_SESSIONS = 20
RVOL_MIN_SESSIONS = 5  # require at least 5 sessions for valid baseline
RVOL_MIN_BASELINE_VOLUME = 100  # minimum baseline volume to compute RVOL (absolute floor)

# Slot counts for different timeframes
SLOTS_5MIN = 78  # 6.5 hours * 12 slots/hour
SLOTS_15MIN = 26  # 6.5 hours * 4 slots/hour
SLOTS_1MIN_FIRST_HOUR = 60  # 60 minutes in first hour

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
    is_poc: bool = False
    is_value_area: bool = False


@dataclass
class VolumeProfileMetrics:
    """Data class for volume profile metrics"""
    poc_price: float
    value_area_high: float
    value_area_low: float
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
    session_date: date
    lookback_days: int
    composite_poc: float
    composite_vah: float
    composite_val: float
    total_volume: int
    hvn_levels: List[float]
    lvn_levels: List[float]
    imbalance_score: float


@njit(cache=True)
def _distribute_volume_exact(lows, highs, volumes, bin_lowers, bin_uppers, price_levels):
    """Numba-accelerated volume distribution."""
    volume_at_price = np.zeros(price_levels, dtype=np.float64)

    for i in range(len(volumes)):
        if volumes[i] == 0:
            continue

        bar_low = lows[i]
        bar_high = highs[i]
        bar_vol = volumes[i]

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
    """Numba-accelerated value area calculation."""
    poc_index = 0
    max_vol = volume_at_price[0]
    for i in range(1, price_levels):
        if volume_at_price[i] > max_vol:
            max_vol = volume_at_price[i]
            poc_index = i

    total_volume = volume_at_price.sum()
    value_area_volume = total_volume * 0.70

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

        if expand_high and (not expand_low or high_vol >= low_vol):
            high_index += 1
            current_volume += volume_at_price[high_index]
        elif expand_low:
            low_index -= 1
            current_volume += volume_at_price[low_index]

    return poc_index, low_index, high_index, total_volume


class AlpacaDataCollectorOptimized:
    def __init__(self, api_key: str, secret_key: str):
        """Initialize the Alpaca data collector."""
        self.client = StockHistoricalDataClient(api_key, secret_key)
        self.pst_tz = pytz.timezone('US/Pacific')
        self.est_tz = pytz.timezone('US/Eastern')
        self.utc_tz = pytz.UTC

        self.db_lock = threading.Lock()
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
            # candles_5min with RVOL fields and TrendEngine fields
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS candles_5min
                        (
                            id
                            SERIAL
                            PRIMARY
                            KEY,
                            symbol
                            TEXT
                            NOT
                            NULL,
                            timestamp
                            TIMESTAMP
                            NOT
                            NULL,
                            open
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            high
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            low
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            close
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            volume
                            BIGINT
                            NOT
                            NULL,
                            vwap
                            DOUBLE
                            PRECISION,
                            ema_8
                            DOUBLE
                            PRECISION,
                            ema_20
                            DOUBLE
                            PRECISION,
                            ema_39
                            DOUBLE
                            PRECISION,
                            slot_index
                            SMALLINT,
                            cum_volume
                            BIGINT,
                            rvol_slot_20
                            DOUBLE
                            PRECISION,
                            rvol_slot_baseline_20
                            DOUBLE
                            PRECISION,
                            rvol_cum_20
                            DOUBLE
                            PRECISION,
                            rvol_cum_baseline_20
                            DOUBLE
                            PRECISION,
                            intraday_rvol_20
                            DOUBLE
                            PRECISION,
                            avg_daily_volume_20
                            DOUBLE
                            PRECISION,
                            pct_of_day_typical
                            DOUBLE
                            PRECISION,
                            created_at
                            TIMESTAMP
                            DEFAULT
                            CURRENT_TIMESTAMP,
                            UNIQUE
                        (
                            symbol,
                            timestamp
                        )
                            );
                        """)

            # Add RVOL columns and TrendEngine columns to candles_5min if table already exists
            new_columns_5min = [
                ("slot_index", "SMALLINT"),
                ("cum_volume", "BIGINT"),
                ("rvol_slot_20", "DOUBLE PRECISION"),
                ("rvol_slot_baseline_20", "DOUBLE PRECISION"),
                ("rvol_cum_20", "DOUBLE PRECISION"),
                ("rvol_cum_baseline_20", "DOUBLE PRECISION"),
                ("intraday_rvol_20", "DOUBLE PRECISION"),
                ("avg_daily_volume_20", "DOUBLE PRECISION"),
                ("pct_of_day_typical", "DOUBLE PRECISION"),
                ("ema_50", "DOUBLE PRECISION"),
                ("ema_39_slope", "DOUBLE PRECISION"),
                ("ema_50_slope", "DOUBLE PRECISION"),
            ]
            for col_name, col_type in new_columns_5min:
                cur.execute(f"ALTER TABLE candles_5min ADD COLUMN IF NOT EXISTS {col_name} {col_type};")

            # candles_15min with RVOL fields and TrendEngine fields
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS candles_15min
                        (
                            id
                            SERIAL
                            PRIMARY
                            KEY,
                            symbol
                            TEXT
                            NOT
                            NULL,
                            timestamp
                            TIMESTAMP
                            NOT
                            NULL,
                            open
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            high
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            low
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            close
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            volume
                            BIGINT
                            NOT
                            NULL,
                            vwap
                            DOUBLE
                            PRECISION,
                            ema_8
                            DOUBLE
                            PRECISION,
                            ema_20
                            DOUBLE
                            PRECISION,
                            ema_39
                            DOUBLE
                            PRECISION,
                            slot_index
                            SMALLINT,
                            cum_volume
                            BIGINT,
                            rvol_slot_20
                            DOUBLE
                            PRECISION,
                            rvol_slot_baseline_20
                            DOUBLE
                            PRECISION,
                            rvol_cum_20
                            DOUBLE
                            PRECISION,
                            rvol_cum_baseline_20
                            DOUBLE
                            PRECISION,
                            intraday_rvol_20
                            DOUBLE
                            PRECISION,
                            avg_daily_volume_20
                            DOUBLE
                            PRECISION,
                            pct_of_day_typical
                            DOUBLE
                            PRECISION,
                            created_at
                            TIMESTAMP
                            DEFAULT
                            CURRENT_TIMESTAMP,
                            UNIQUE
                        (
                            symbol,
                            timestamp
                        )
                            );
                        """)

            # Add columns to candles_15min if table already exists (including TrendEngine)
            new_columns_15min = [
                ("slot_index", "SMALLINT"),
                ("cum_volume", "BIGINT"),
                ("rvol_slot_20", "DOUBLE PRECISION"),
                ("rvol_slot_baseline_20", "DOUBLE PRECISION"),
                ("rvol_cum_20", "DOUBLE PRECISION"),
                ("rvol_cum_baseline_20", "DOUBLE PRECISION"),
                ("intraday_rvol_20", "DOUBLE PRECISION"),
                ("avg_daily_volume_20", "DOUBLE PRECISION"),
                ("pct_of_day_typical", "DOUBLE PRECISION"),
                ("ema_50", "DOUBLE PRECISION"),
                ("ema_39_slope", "DOUBLE PRECISION"),
                ("ema_50_slope", "DOUBLE PRECISION"),
            ]
            for col_name, col_type in new_columns_15min:
                cur.execute(f"ALTER TABLE candles_15min ADD COLUMN IF NOT EXISTS {col_name} {col_type};")

            # NEW: candles_1min_first_hour - First hour 1-minute candles
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS candles_1min_first_hour
                        (
                            id
                            SERIAL
                            PRIMARY
                            KEY,
                            symbol
                            TEXT
                            NOT
                            NULL,
                            timestamp
                            TIMESTAMP
                            NOT
                            NULL,
                            open
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            high
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            low
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            close
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            volume
                            BIGINT
                            NOT
                            NULL,
                            vwap
                            DOUBLE
                            PRECISION,

                            -- EMAs
                            ema_8
                            DOUBLE
                            PRECISION,
                            ema_20
                            DOUBLE
                            PRECISION,
                            ema_39
                            DOUBLE
                            PRECISION,
                            ema_50
                            DOUBLE
                            PRECISION,
                            ema_39_slope
                            DOUBLE
                            PRECISION,
                            ema_50_slope
                            DOUBLE
                            PRECISION,

                            -- Slot RVOL (within first hour context)
                            slot_index
                            SMALLINT,
                            cum_volume
                            BIGINT,
                            rvol_slot_20
                            DOUBLE
                            PRECISION,
                            rvol_slot_baseline_20
                            DOUBLE
                            PRECISION,

                            -- Cumulative RVOL (within first hour)
                            rvol_cum_20
                            DOUBLE
                            PRECISION,
                            rvol_cum_baseline_20
                            DOUBLE
                            PRECISION,

                            -- Initial Balance metrics (computed at end of first hour)
                            ib_high
                            DOUBLE
                            PRECISION,
                            ib_low
                            DOUBLE
                            PRECISION,
                            ib_range
                            DOUBLE
                            PRECISION,

                            created_at
                            TIMESTAMP
                            DEFAULT
                            CURRENT_TIMESTAMP,
                            UNIQUE
                        (
                            symbol,
                            timestamp
                        )
                            );
                        """)

            # daily_bars with TrendEngine fields
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS daily_bars
                        (
                            id
                            SERIAL
                            PRIMARY
                            KEY,
                            symbol
                            TEXT
                            NOT
                            NULL,
                            timestamp
                            DATE
                            NOT
                            NULL,
                            open
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            high
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            low
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            close
                            DOUBLE
                            PRECISION
                            NOT
                            NULL,
                            volume
                            BIGINT
                            NOT
                            NULL,
                            vwap
                            DOUBLE
                            PRECISION,
                            vwap_upper
                            DOUBLE
                            PRECISION,
                            vwap_lower
                            DOUBLE
                            PRECISION,
                            session_high
                            DOUBLE
                            PRECISION,
                            session_low
                            DOUBLE
                            PRECISION,
                            poc
                            DOUBLE
                            PRECISION,
                            atr
                            DOUBLE
                            PRECISION,
                            rsi_10
                            DOUBLE
                            PRECISION,
                            momentum_10
                            DOUBLE
                            PRECISION,
                            ema_8
                            DOUBLE
                            PRECISION,
                            ema_20
                            DOUBLE
                            PRECISION,
                            ema_39_of_15min
                            DOUBLE
                            PRECISION,
                            created_at
                            TIMESTAMP
                            DEFAULT
                            CURRENT_TIMESTAMP,
                            UNIQUE
                        (
                            symbol,
                            timestamp
                        )
                            );
                        """)

            # Add new daily_bars columns for TrendEngine (EMA39, EMA50, ADX, DI+/DI-, slopes)
            new_daily_columns = [
                ("ema_39", "DOUBLE PRECISION"),
                ("ema_50", "DOUBLE PRECISION"),
                ("ema_39_slope", "DOUBLE PRECISION"),
                ("ema_50_slope", "DOUBLE PRECISION"),
                ("adx", "DOUBLE PRECISION"),
                ("adx_sma", "DOUBLE PRECISION"),
                ("adx_slope", "DOUBLE PRECISION"),
                ("di_plus", "DOUBLE PRECISION"),
                ("di_minus", "DOUBLE PRECISION"),
            ]
            for col_name, col_type in new_daily_columns:
                cur.execute(f"ALTER TABLE daily_bars ADD COLUMN IF NOT EXISTS {col_name} {col_type};")

            # composite_session_profiles
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS composite_session_profiles
                        (
                            id
                            SERIAL
                            PRIMARY
                            KEY,
                            symbol
                            TEXT
                            NOT
                            NULL,
                            session_date
                            DATE
                            NOT
                            NULL,
                            lookback_days
                            SMALLINT
                            NOT
                            NULL,
                            composite_poc
                            DOUBLE
                            PRECISION,
                            composite_vah
                            DOUBLE
                            PRECISION,
                            composite_val
                            DOUBLE
                            PRECISION,
                            total_volume
                            BIGINT,
                            hvn_levels
                            JSONB,
                            lvn_levels
                            JSONB,
                            imbalance_score
                            DOUBLE
                            PRECISION,
                            created_at
                            TIMESTAMP
                            DEFAULT
                            CURRENT_TIMESTAMP,
                            UNIQUE
                        (
                            symbol,
                            session_date,
                            lookback_days
                        )
                            );
                        """)

            # Indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_5min_symbol_timestamp ON candles_5min(symbol, timestamp);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_5min_symbol_slot ON candles_5min(symbol, slot_index);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_15min_symbol_timestamp ON candles_15min(symbol, timestamp);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_15min_symbol_slot ON candles_15min(symbol, slot_index);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_daily_symbol_timestamp ON daily_bars(symbol, timestamp);")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_csp_symbol_date ON composite_session_profiles(symbol, session_date);")

            # NEW: Indexes for first hour candles
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_1min_fh_symbol_timestamp ON candles_1min_first_hour(symbol, timestamp);")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_1min_fh_symbol_slot ON candles_1min_first_hour(symbol, slot_index);")

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

    def is_first_hour(self, timestamp: datetime) -> bool:
        """Check if timestamp is within first hour (9:30-10:30 AM ET)."""
        if timestamp.tzinfo is None:
            timestamp = self.utc_tz.localize(timestamp)

        et_time = timestamp.astimezone(self.est_tz)
        total_minutes = et_time.hour * 60 + et_time.minute

        return FIRST_HOUR_START_MINUTES <= total_minutes < FIRST_HOUR_END_MINUTES

    # -----------------------------------------------------------------------
    # RVOL Implementation (Generalized for 5min, 15min, and 1min first hour)
    # -----------------------------------------------------------------------

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

        h = ts_et.hour
        m = ts_et.minute
        total_min = h * 60 + m

        start = 9 * 60 + 30  # 09:30 ET
        end = 16 * 60  # 16:00 ET

        if total_min < start or total_min >= end:
            return None

        return int((total_min - start) // interval_minutes)

    def _compute_first_hour_slot_index(self, ts: pd.Timestamp) -> Optional[int]:
        """
        Compute slot_index for first hour (09:30-10:30 ET).
        Returns 0-59 for the 60 one-minute slots.

        Args:
            ts: Timestamp in UTC

        Returns:
            slot_index (0-59), or None if outside first hour.
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

    def calculate_rvol(
            self,
            df: pd.DataFrame,
            interval_minutes: int,
            lookback_sessions: int = RVOL_LOOKBACK_SESSIONS
    ) -> pd.DataFrame:
        """
        Calculate all three RVOL variants for intraday data (5min or 15min):

        1. Slot RVOL: volume at this slot / median volume at same slot over N prior sessions
        2. Cumulative RVOL: cum volume at slot / median cum volume at same slot over N prior sessions
        3. Intraday RVOL: projected EOD volume / average daily volume

        Uses MEDIAN for slot/cum baselines to reduce sensitivity to outlier days (earnings, news).
        Baselines exclude current session (shift by 1).

        Args:
            df: DataFrame with timestamp, volume columns
            interval_minutes: 5 or 15 for the candle interval
            lookback_sessions: Number of prior sessions for baseline calculation

        Returns:
            DataFrame with RVOL columns added

        IMPORTANT: Timestamps remain in UTC. Slot calculations use ET internally.
        """
        if df.empty:
            return df

        # Determine number of slots based on interval
        num_slots = SLOTS_5MIN if interval_minutes == 5 else SLOTS_15MIN

        df = df.copy()
        df = df.sort_values("timestamp").reset_index(drop=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Ensure timestamps are UTC (Alpaca returns UTC)
        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
        else:
            df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")

        # Compute session date (in ET) for each row - used for grouping only
        df["_session_date"] = df["timestamp"].apply(self._get_session_date_from_utc)

        # Slot index (computed from UTC -> ET conversion)
        df["slot_index"] = df["timestamp"].apply(
            lambda ts: self._compute_slot_index_from_utc(ts, interval_minutes)
        )

        # Keep only regular session slots
        df = df[df["slot_index"].notna()].copy()
        if df.empty:
            return df

        df["slot_index"] = df["slot_index"].astype(int)

        # Cumulative volume within session
        df = df.sort_values(["_session_date", "timestamp"]).reset_index(drop=True)
        df["cum_volume"] = df.groupby("_session_date")["volume"].cumsum()

        # Daily total volume per session
        daily_totals = df.groupby("_session_date")["volume"].sum().reset_index()
        daily_totals.columns = ["_session_date", "daily_total_volume"]

        # Get unique sessions in order
        sessions = df["_session_date"].unique()
        sessions = sorted(sessions)
        session_to_idx = {s: i for i, s in enumerate(sessions)}
        df["_session_idx"] = df["_session_date"].map(session_to_idx)

        # -----------------------------------------------------------------
        # 1. SLOT RVOL (using median)
        # -----------------------------------------------------------------
        slot_baselines = []
        for slot in range(num_slots):
            slot_df = df[df["slot_index"] == slot][["_session_date", "_session_idx", "volume"]].drop_duplicates(
                "_session_date")
            slot_df = slot_df.sort_values("_session_idx").reset_index(drop=True)

            # Rolling median over prior sessions (shift excludes current)
            slot_df["baseline"] = (
                slot_df["volume"]
                .shift(1)
                .rolling(window=lookback_sessions, min_periods=RVOL_MIN_SESSIONS)
                .median()
            )
            slot_df["slot_index"] = slot
            slot_baselines.append(slot_df[["_session_date", "slot_index", "baseline"]])

        slot_baseline_df = pd.concat(slot_baselines, ignore_index=True)
        slot_baseline_df.columns = ["_session_date", "slot_index", "rvol_slot_baseline_20"]

        df = df.merge(slot_baseline_df, on=["_session_date", "slot_index"], how="left")

        # Apply minimum baseline floor
        df["rvol_slot_baseline_20"] = df["rvol_slot_baseline_20"].clip(lower=RVOL_MIN_BASELINE_VOLUME)

        # Calculate slot RVOL
        df["rvol_slot_20"] = np.where(
            df["rvol_slot_baseline_20"].notna() & (df["rvol_slot_baseline_20"] > 0),
            df["volume"] / df["rvol_slot_baseline_20"],
            np.nan
        )

        # -----------------------------------------------------------------
        # 2. CUMULATIVE RVOL (using median)
        # -----------------------------------------------------------------
        cum_baselines = []
        for slot in range(num_slots):
            slot_df = df[df["slot_index"] == slot][["_session_date", "_session_idx", "cum_volume"]].drop_duplicates(
                "_session_date")
            slot_df = slot_df.sort_values("_session_idx").reset_index(drop=True)

            slot_df["baseline"] = (
                slot_df["cum_volume"]
                .shift(1)
                .rolling(window=lookback_sessions, min_periods=RVOL_MIN_SESSIONS)
                .median()
            )
            slot_df["slot_index"] = slot
            cum_baselines.append(slot_df[["_session_date", "slot_index", "baseline"]])

        cum_baseline_df = pd.concat(cum_baselines, ignore_index=True)
        cum_baseline_df.columns = ["_session_date", "slot_index", "rvol_cum_baseline_20"]

        df = df.merge(cum_baseline_df, on=["_session_date", "slot_index"], how="left")

        df["rvol_cum_baseline_20"] = df["rvol_cum_baseline_20"].clip(lower=RVOL_MIN_BASELINE_VOLUME)

        df["rvol_cum_20"] = np.where(
            df["rvol_cum_baseline_20"].notna() & (df["rvol_cum_baseline_20"] > 0),
            df["cum_volume"] / df["rvol_cum_baseline_20"],
            np.nan
        )

        # -----------------------------------------------------------------
        # 3. INTRADAY RVOL (projected EOD volume / average daily volume)
        # -----------------------------------------------------------------

        # Average daily volume over prior N sessions
        daily_totals = daily_totals.sort_values("_session_date").reset_index(drop=True)
        daily_totals["avg_daily_volume_20"] = (
            daily_totals["daily_total_volume"]
            .shift(1)
            .rolling(window=lookback_sessions, min_periods=RVOL_MIN_SESSIONS)
            .mean()
        )

        # Merge daily totals
        df = df.merge(
            daily_totals[["_session_date", "daily_total_volume", "avg_daily_volume_20"]],
            on="_session_date",
            how="left"
        )

        # pct of day completed at this slot for THIS session
        df["_pct_of_day_actual"] = np.where(
            df["daily_total_volume"] > 0,
            df["cum_volume"] / df["daily_total_volume"],
            np.nan
        )

        # Compute typical % at each slot (median across prior sessions)
        pct_baselines = []
        for slot in range(num_slots):
            slot_df = df[df["slot_index"] == slot][
                ["_session_date", "_session_idx", "_pct_of_day_actual"]].drop_duplicates("_session_date")
            slot_df = slot_df.sort_values("_session_idx").reset_index(drop=True)

            slot_df["pct_typical"] = (
                slot_df["_pct_of_day_actual"]
                .shift(1)
                .rolling(window=lookback_sessions, min_periods=RVOL_MIN_SESSIONS)
                .median()
            )
            slot_df["slot_index"] = slot
            pct_baselines.append(slot_df[["_session_date", "slot_index", "pct_typical"]])

        pct_baseline_df = pd.concat(pct_baselines, ignore_index=True)
        pct_baseline_df.columns = ["_session_date", "slot_index", "pct_of_day_typical"]

        df = df.merge(pct_baseline_df, on=["_session_date", "slot_index"], how="left")

        # Projected EOD = cum_volume / pct_of_day_typical
        # Clamp pct_of_day_typical to avoid wild projections early in day
        df["_pct_of_day_typical_clamped"] = df["pct_of_day_typical"].clip(lower=0.05)

        df["_projected_eod_volume"] = np.where(
            df["_pct_of_day_typical_clamped"].notna() & (df["_pct_of_day_typical_clamped"] > 0),
            df["cum_volume"] / df["_pct_of_day_typical_clamped"],
            np.nan
        )

        # Intraday RVOL = projected EOD / avg daily volume
        df["intraday_rvol_20"] = np.where(
            df["avg_daily_volume_20"].notna() & (df["avg_daily_volume_20"] > RVOL_MIN_BASELINE_VOLUME),
            df["_projected_eod_volume"] / df["avg_daily_volume_20"],
            np.nan
        )

        # -----------------------------------------------------------------
        # Round and clean up
        # -----------------------------------------------------------------
        df["rvol_slot_20"] = df["rvol_slot_20"].round(4)
        df["rvol_cum_20"] = df["rvol_cum_20"].round(4)
        df["intraday_rvol_20"] = df["intraday_rvol_20"].round(4)
        df["rvol_slot_baseline_20"] = df["rvol_slot_baseline_20"].round(2)
        df["rvol_cum_baseline_20"] = df["rvol_cum_baseline_20"].round(2)
        df["avg_daily_volume_20"] = df["avg_daily_volume_20"].round(2)
        df["pct_of_day_typical"] = df["pct_of_day_typical"].round(4)

        # Drop intermediate columns (prefixed with _)
        cols_to_drop = [
            "_session_idx", "_session_date", "daily_total_volume", "_pct_of_day_actual",
            "_pct_of_day_typical_clamped", "_projected_eod_volume"
        ]
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

        # Restore original sort by timestamp
        df = df.sort_values("timestamp").reset_index(drop=True)

        return df

    def calculate_rvol_first_hour(
            self,
            df: pd.DataFrame,
            lookback_sessions: int = RVOL_LOOKBACK_SESSIONS
    ) -> pd.DataFrame:
        """
        Calculate RVOL for first hour 1-minute candles.

        1. Slot RVOL: volume at this minute / median volume at same minute over N prior sessions
        2. Cumulative RVOL: cum volume at minute / median cum volume at same minute over N prior sessions

        Args:
            df: DataFrame with timestamp, volume columns (1-minute first hour data)
            lookback_sessions: Number of prior sessions for baseline calculation

        Returns:
            DataFrame with RVOL columns and IB metrics added
        """
        if df.empty:
            return df

        num_slots = SLOTS_1MIN_FIRST_HOUR  # 60 slots

        df = df.copy()
        df = df.sort_values("timestamp").reset_index(drop=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Ensure timestamps are UTC
        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
        else:
            df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")

        # Compute session date for grouping
        df["_session_date"] = df["timestamp"].apply(self._get_session_date_from_utc)

        # Slot index (0-59 for first hour)
        df["slot_index"] = df["timestamp"].apply(self._compute_first_hour_slot_index)

        # Keep only first hour slots
        df = df[df["slot_index"].notna()].copy()
        if df.empty:
            return df

        df["slot_index"] = df["slot_index"].astype(int)

        # Cumulative volume within first hour
        df = df.sort_values(["_session_date", "timestamp"]).reset_index(drop=True)
        df["cum_volume"] = df.groupby("_session_date")["volume"].cumsum()

        # Get unique sessions in order
        sessions = sorted(df["_session_date"].unique())
        session_to_idx = {s: i for i, s in enumerate(sessions)}
        df["_session_idx"] = df["_session_date"].map(session_to_idx)

        # -----------------------------------------------------------------
        # 1. SLOT RVOL (using median)
        # -----------------------------------------------------------------
        slot_baselines = []
        for slot in range(num_slots):
            slot_df = df[df["slot_index"] == slot][["_session_date", "_session_idx", "volume"]].drop_duplicates(
                "_session_date")
            slot_df = slot_df.sort_values("_session_idx").reset_index(drop=True)

            slot_df["baseline"] = (
                slot_df["volume"]
                .shift(1)
                .rolling(window=lookback_sessions, min_periods=RVOL_MIN_SESSIONS)
                .median()
            )
            slot_df["slot_index"] = slot
            slot_baselines.append(slot_df[["_session_date", "slot_index", "baseline"]])

        slot_baseline_df = pd.concat(slot_baselines, ignore_index=True)
        slot_baseline_df.columns = ["_session_date", "slot_index", "rvol_slot_baseline_20"]

        df = df.merge(slot_baseline_df, on=["_session_date", "slot_index"], how="left")
        df["rvol_slot_baseline_20"] = df["rvol_slot_baseline_20"].clip(lower=RVOL_MIN_BASELINE_VOLUME)

        df["rvol_slot_20"] = np.where(
            df["rvol_slot_baseline_20"].notna() & (df["rvol_slot_baseline_20"] > 0),
            df["volume"] / df["rvol_slot_baseline_20"],
            np.nan
        )

        # -----------------------------------------------------------------
        # 2. CUMULATIVE RVOL (using median)
        # -----------------------------------------------------------------
        cum_baselines = []
        for slot in range(num_slots):
            slot_df = df[df["slot_index"] == slot][["_session_date", "_session_idx", "cum_volume"]].drop_duplicates(
                "_session_date")
            slot_df = slot_df.sort_values("_session_idx").reset_index(drop=True)

            slot_df["baseline"] = (
                slot_df["cum_volume"]
                .shift(1)
                .rolling(window=lookback_sessions, min_periods=RVOL_MIN_SESSIONS)
                .median()
            )
            slot_df["slot_index"] = slot
            cum_baselines.append(slot_df[["_session_date", "slot_index", "baseline"]])

        cum_baseline_df = pd.concat(cum_baselines, ignore_index=True)
        cum_baseline_df.columns = ["_session_date", "slot_index", "rvol_cum_baseline_20"]

        df = df.merge(cum_baseline_df, on=["_session_date", "slot_index"], how="left")
        df["rvol_cum_baseline_20"] = df["rvol_cum_baseline_20"].clip(lower=RVOL_MIN_BASELINE_VOLUME)

        df["rvol_cum_20"] = np.where(
            df["rvol_cum_baseline_20"].notna() & (df["rvol_cum_baseline_20"] > 0),
            df["cum_volume"] / df["rvol_cum_baseline_20"],
            np.nan
        )

        # -----------------------------------------------------------------
        # 3. Initial Balance (IB) metrics - computed per session
        # -----------------------------------------------------------------
        ib_stats = df.groupby("_session_date").agg(
            ib_high=("high", "max"),
            ib_low=("low", "min")
        ).reset_index()
        ib_stats["ib_range"] = ib_stats["ib_high"] - ib_stats["ib_low"]

        df = df.merge(ib_stats, on="_session_date", how="left")

        # -----------------------------------------------------------------
        # Round and clean up
        # -----------------------------------------------------------------
        df["rvol_slot_20"] = df["rvol_slot_20"].round(4)
        df["rvol_cum_20"] = df["rvol_cum_20"].round(4)
        df["rvol_slot_baseline_20"] = df["rvol_slot_baseline_20"].round(2)
        df["rvol_cum_baseline_20"] = df["rvol_cum_baseline_20"].round(2)
        df["ib_high"] = df["ib_high"].round(2)
        df["ib_low"] = df["ib_low"].round(2)
        df["ib_range"] = df["ib_range"].round(2)

        # Drop intermediate columns
        cols_to_drop = ["_session_idx", "_session_date"]
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

        df = df.sort_values("timestamp").reset_index(drop=True)

        return df

    def calculate_rvol_5m(
            self,
            df: pd.DataFrame,
            lookback_sessions: int = RVOL_LOOKBACK_SESSIONS
    ) -> pd.DataFrame:
        """
        Calculate RVOL for 5-minute candles.
        Convenience wrapper around calculate_rvol.
        """
        return self.calculate_rvol(df, interval_minutes=5, lookback_sessions=lookback_sessions)

    def calculate_rvol_15m(
            self,
            df: pd.DataFrame,
            lookback_sessions: int = RVOL_LOOKBACK_SESSIONS
    ) -> pd.DataFrame:
        """
        Calculate RVOL for 15-minute candles.
        Convenience wrapper around calculate_rvol.
        """
        return self.calculate_rvol(df, interval_minutes=15, lookback_sessions=lookback_sessions)

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
        """Calculate VWAP for each bar with optional bands."""
        if df.empty:
            return df

        df = df.copy()
        df = df.sort_values("timestamp").reset_index(drop=True)

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["pv"] = df["typical_price"] * df["volume"]

        # Get date in ET for session grouping (but don't modify timestamp)
        if df["timestamp"].dt.tz is None:
            ts_for_date = df["timestamp"].dt.tz_localize("UTC")
        else:
            ts_for_date = df["timestamp"].dt.tz_convert("UTC")

        df["_date_et"] = ts_for_date.dt.tz_convert(self.est_tz).dt.date

        df["cum_pv"] = df.groupby("_date_et")["pv"].cumsum()
        df["cum_volume"] = df.groupby("_date_et")["volume"].cumsum()

        df["vwap"] = 0.0
        mask = df["cum_volume"] > 0
        df.loc[mask, "vwap"] = (df.loc[mask, "cum_pv"] / df.loc[mask, "cum_volume"]).round(2)

        if include_bands:
            df["vwap_upper"] = df["vwap"]
            df["vwap_lower"] = df["vwap"]

            for date_val in df["_date_et"].unique():
                date_mask = df["_date_et"] == date_val
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
                        variance = np.sum(weights * (typical_prices[: i + 1] - weighted_mean) ** 2)
                        std_dev = np.sqrt(variance) if variance > 0 else 0

                        upper_band.append(weighted_mean + multiplier * std_dev)
                        lower_band.append(weighted_mean - multiplier * std_dev)
                    else:
                        upper_band.append(vwap_values[i] if i < len(vwap_values) else 0)
                        lower_band.append(vwap_values[i] if i < len(vwap_values) else 0)

                df.loc[date_mask, "vwap_upper"] = np.round(upper_band, 2)
                df.loc[date_mask, "vwap_lower"] = np.round(lower_band, 2)

        columns_to_drop = ["typical_price", "pv", "cum_pv", "cum_volume", "_date_et"]
        df = df.drop(columns=[c for c in columns_to_drop if c in df.columns], axis=1)

        return df

    def calculate_ema_series(self, df: pd.DataFrame, periods: List[int]) -> pd.DataFrame:
        """Calculate Exponential Moving Averages for given periods."""
        if df.empty:
            return df

        df = df.copy()
        df = df.sort_values("timestamp").reset_index(drop=True)

        for period in periods:
            df[f"ema_{period}"] = df["close"].ewm(span=period, adjust=False).mean().round(3)

        return df

    def calculate_adx_daily(
            self,
            df: pd.DataFrame,
            length: int = 14,
            avg_length: int = 5
    ) -> pd.DataFrame:
        """
        Calculate DI+, DI-, ADX, and SMA(ADX) using Wilder-style smoothing for daily bars.
        """
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
            if sum_di == 0:
                dx = 0.0
            else:
                dx = (abs(di_plus - di_minus) / sum_di) * 100.0

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

    def calculate_slopes(self, df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
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

    def calculate_intraday_slopes(self, df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
        """Calculate slopes for EMA39 and EMA50 for intraday data."""
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

    # -----------------------------------------------------------------------
    # Session stats & volume profile
    # -----------------------------------------------------------------------

    def resample_to_daily_bars(self, day_dfs: Dict[date, pd.DataFrame], symbol: str) -> pd.DataFrame:
        """Resample 1-minute market hours data to daily bars."""
        daily_records = []

        for d, day_df in sorted(day_dfs.items()):
            if day_df.empty:
                continue

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

            if daily_record["volume"] > 0:
                typical_price = (day_df["high"] + day_df["low"] + day_df["close"]) / 3
                daily_record["vwap"] = round(
                    float((typical_price * day_df["volume"]).sum() / daily_record["volume"]), 2
                )
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
    ) -> Tuple[Dict[str, Optional[SessionStats]], pd.DataFrame, pd.DataFrame]:
        """
        Fetch and process multiple days of session data at once.

        Returns:
            Tuple of (session_stats_dict, daily_bars_df, first_hour_1min_df)
        """
        if not dates:
            return {}, pd.DataFrame(), pd.DataFrame()

        dates_as_date = sorted([datetime.strptime(d, "%Y-%m-%d").date() for d in dates])
        est = self.est_tz

        start_date = dates_as_date[0]
        end_date = dates_as_date[-1]

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
            adjustment=Adjustment.ALL
        )

        try:
            self.api_call_count += 1
            bars = self.client.get_stock_bars(request)
            df = bars.df.reset_index()

            if df.empty:
                return {d.isoformat(): None for d in dates_as_date}, pd.DataFrame(), pd.DataFrame()

            if "timestamp" not in df.columns:
                logging.error(f"No timestamp column found for {symbol}")
                return {d.isoformat(): None for d in dates_as_date}, pd.DataFrame(), pd.DataFrame()

            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Convert to ET for filtering
            df["_timestamp_est"] = df["timestamp"].dt.tz_convert(est)
            df["_time_decimal"] = df["_timestamp_est"].dt.hour + df["_timestamp_est"].dt.minute / 60.0

            # Filter to regular hours (9:30 AM - 4:00 PM ET)
            regular_hours_df = df[(df["_time_decimal"] >= 9.5) & (df["_time_decimal"] < 16.0)].copy()

            # Extract first hour data (9:30 - 10:30 AM ET)
            first_hour_df = df[(df["_time_decimal"] >= 9.5) & (df["_time_decimal"] < 10.5)].copy()

            # Get date in ET for grouping
            regular_hours_df["date"] = regular_hours_df["_timestamp_est"].dt.date
            first_hour_df["date"] = first_hour_df["_timestamp_est"].dt.date

            regular_hours_df = regular_hours_df.drop(columns=["_timestamp_est", "_time_decimal"])
            first_hour_df = first_hour_df.drop(columns=["_timestamp_est", "_time_decimal"])

            if regular_hours_df.empty:
                return {d.isoformat(): None for d in dates_as_date}, pd.DataFrame(), pd.DataFrame()

            day_dfs: Dict[date, pd.DataFrame] = {}
            for d in dates_as_date:
                day_df = regular_hours_df[regular_hours_df["date"] == d].copy()
                if not day_df.empty:
                    day_df = self.calculate_vwap_series(day_df, include_bands=True)
                day_dfs[d] = day_df

            # Process first hour data for storage
            first_hour_all = []
            for d in dates_as_date:
                fh_day = first_hour_df[first_hour_df["date"] == d].copy()
                if not fh_day.empty:
                    fh_day = fh_day.drop(columns=["date"], errors="ignore")
                    fh_day["symbol"] = symbol
                    first_hour_all.append(fh_day)

            if first_hour_all:
                first_hour_combined = pd.concat(first_hour_all, ignore_index=True)
                # Calculate VWAP for first hour
                first_hour_combined = self.calculate_vwap_series(first_hour_combined)
                # Calculate EMAs
                first_hour_combined = self.calculate_ema_series(first_hour_combined, [8, 20, 39, 50])
                # Calculate slopes
                first_hour_combined = self.calculate_intraday_slopes(first_hour_combined, window=3)
                # Calculate RVOL for first hour
                first_hour_combined = self.calculate_rvol_first_hour(first_hour_combined)
            else:
                first_hour_combined = pd.DataFrame()

        except Exception as e:
            logging.error(f"Failed to fetch session data for {symbol}: {e}")
            return {d.isoformat(): None for d in dates_as_date}, pd.DataFrame(), pd.DataFrame()

        results: Dict[str, Optional[SessionStats]] = {}
        profiles_to_store: List[CompositeSessionProfile] = []

        def _build_composite(date_idx: int, lookback_days: int) -> Optional[CompositeSessionProfile]:
            end_idx = date_idx
            start_idx = date_idx - lookback_days
            if start_idx < 0:
                return None

            window_dates = dates_as_date[start_idx:end_idx]
            dfs_to_combine = [day_dfs[d] for d in window_dates if not day_dfs[d].empty]
            if not dfs_to_combine:
                return None

            comp_df = pd.concat(dfs_to_combine, ignore_index=True)
            if comp_df.empty:
                return None

            try:
                vp = self.calculate_volume_profile_optimized(comp_df, price_levels=70)
                vols = vp.price_levels
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
                    session_date=dates_as_date[date_idx],
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
                    f"Failed composite profile for {symbol} lb={lookback_days} on {dates_as_date[date_idx]}: {e}"
                )
                return None

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
                logging.warning(f"Could not calculate volume profile for {symbol} on {date_str}: {e}")
                session_high = day_df["high"].max()
                session_low = day_df["low"].min()
                poc = day_df.loc[day_df["volume"].idxmax(), "close"] if not day_df.empty else None

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

            for lb in [2, 3, 4]:
                profile = _build_composite(idx, lb)
                if profile:
                    profiles_to_store.append(profile)

        if profiles_to_store:
            self._store_composite_session_profiles_batch(profiles_to_store)

        daily_bars_df = self.resample_to_daily_bars(day_dfs, symbol)

        return results, daily_bars_df, first_hour_combined

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
            logging.debug(f"Batch stored {len(profiles)} composite profiles")

        except Exception as e:
            conn.rollback()
            logging.error(f"Error batch storing composite profiles: {e}")
        finally:
            cur.close()
            conn.close()

    def calculate_volume_profile_optimized(
            self, df: pd.DataFrame, price_levels: int = 70
    ) -> VolumeProfileMetrics:
        """Numba-accelerated volume profile calculation."""
        if df.empty:
            raise ValueError("DataFrame is empty")

        high_price = df["high"].max()
        low_price = df["low"].min()
        price_range = high_price - low_price

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

        lows = df["low"].values.astype(np.float64)
        highs = df["high"].values.astype(np.float64)
        volumes = df["volume"].values.astype(np.float64)

        price_bins = np.linspace(low_price, high_price, price_levels + 1)
        bin_lowers = price_bins[:-1].astype(np.float64)
        bin_uppers = price_bins[1:].astype(np.float64)

        volume_at_price = _distribute_volume_exact(
            lows, highs, volumes, bin_lowers, bin_uppers, price_levels
        )

        poc_index, low_index, high_index, total_volume = _calculate_value_area(
            volume_at_price, price_levels
        )

        poc_price = (price_bins[poc_index] + price_bins[poc_index + 1]) / 2
        value_area_high = (price_bins[high_index] + price_bins[high_index + 1]) / 2
        value_area_low = (price_bins[low_index] + price_bins[low_index + 1]) / 2

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
    # Symbol processing / indicators
    # -----------------------------------------------------------------------

    def process_symbol_batch(self, symbols: List[str], days_back: int = 360) -> Tuple[
        Dict[str, List[dict]], Dict[str, pd.DataFrame]]:
        """
        Process a batch of symbols in parallel.

        Returns:
            Tuple of (daily_bars_dict, first_hour_dict)
        """
        all_daily_bars: Dict[str, List[dict]] = {}
        all_first_hour: Dict[str, pd.DataFrame] = {}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_symbol = {
                executor.submit(self.process_single_symbol, symbol, datetime.now() - timedelta(days=days_back),
                                datetime.now()): symbol
                for symbol in symbols
            }

            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    daily_bars, first_hour_df = future.result()
                    all_daily_bars[symbol] = daily_bars
                    all_first_hour[symbol] = first_hour_df
                    logging.info(f"Processed {symbol}")
                except Exception as e:
                    logging.error(f"Error processing {symbol}: {e}")
                    all_daily_bars[symbol] = []
                    all_first_hour[symbol] = pd.DataFrame()

        return all_daily_bars, all_first_hour

    def process_single_symbol(
            self, symbol: str, start_date: datetime, end_date: datetime
    ) -> Tuple[List[dict], pd.DataFrame]:
        """
        Process a single symbol - fetch daily bars, session stats, and first hour data.

        Returns:
            Tuple of (daily_bars_list, first_hour_df)
        """
        try:
            request = StockBarsRequest(
                symbol_or_symbols=[symbol],
                timeframe=TimeFrame(1, TimeFrameUnit.Day),
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                feed="iex",
                adjustment=Adjustment.ALL)

            self.api_call_count += 1
            bars = self.client.get_stock_bars(request)
            df = bars.df.reset_index()

            if df.empty:
                return [], pd.DataFrame()

            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.sort_values("timestamp").reset_index(drop=True)

            dates_to_fetch = [row["timestamp"].date().isoformat() for _, row in df.iterrows()]
            session_stats, daily_df, first_hour_df = self.fetch_and_process_session_data(symbol, dates_to_fetch)

            if daily_df.empty:
                return [], first_hour_df

            daily_df = self.calculate_daily_indicators_optimized(daily_df)

            result: List[dict] = []
            for _, row in daily_df.iterrows():
                date_str = row["timestamp"].date().isoformat()
                stats = session_stats.get(date_str)

                bar_data = {
                    "symbol": row["symbol"],
                    "timestamp": date_str,
                    "open": float(row["open"]) if pd.notna(row["open"]) else None,
                    "high": float(stats.highest_high) if stats and stats.highest_high else float(
                        row["high"]) if pd.notna(row["high"]) else None,
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
                    "ema_39": float(row.get("ema_39")) if pd.notna(row.get("ema_39")) else None,
                    "ema_50": float(row.get("ema_50")) if pd.notna(row.get("ema_50")) else None,
                    "ema_39_slope": float(row.get("ema_39_slope")) if pd.notna(row.get("ema_39_slope")) else None,
                    "ema_50_slope": float(row.get("ema_50_slope")) if pd.notna(row.get("ema_50_slope")) else None,
                    "adx": float(row.get("adx")) if pd.notna(row.get("adx")) else None,
                    "adx_sma": float(row.get("adx_sma")) if pd.notna(row.get("adx_sma")) else None,
                    "adx_slope": float(row.get("adx_slope")) if pd.notna(row.get("adx_slope")) else None,
                    "di_plus": float(row.get("di_plus")) if pd.notna(row.get("di_plus")) else None,
                    "di_minus": float(row.get("di_minus")) if pd.notna(row.get("di_minus")) else None,
                }
                result.append(bar_data)

            return result, first_hour_df

        except Exception as e:
            logging.error(f"Error processing {symbol}: {e}")
            return [], pd.DataFrame()

    def calculate_daily_indicators_optimized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate all daily indicators using vectorized operations."""
        if df.empty:
            return df

        df = df.copy()

        df["high_low"] = df["high"] - df["low"]
        df["high_pc"] = abs(df["high"] - df["close"].shift(1))
        df["low_pc"] = abs(df["low"] - df["close"].shift(1))
        df["true_range"] = df[["high_low", "high_pc", "low_pc"]].max(axis=1)

        df["atr"] = df["true_range"].ewm(span=10, adjust=False).mean()

        df = self.calculate_rsi(df, period=10)
        df = self.calculate_momentum(df, period=10)
        df = self.calculate_ema_series(df, [8, 20, 39, 50])
        df = self.calculate_adx_daily(df, length=14, avg_length=5)
        df = self.calculate_slopes(df, window=3)

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
                executor.submit(self.get_market_hours_data_single, symbol, timeframe, days_back): symbol
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
            self, symbol: str, timeframe: TimeFrame, days_back: int = 360
    ) -> pd.DataFrame:
        """Fetch market hours data for a single symbol."""
        end_date = datetime.now(self.est_tz)
        start_date = end_date - timedelta(days=days_back)

        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=timeframe,
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            adjustment=Adjustment.ALL)

        try:
            self.api_call_count += 1
            bars = self.client.get_stock_bars(request)
            df = bars.df.reset_index()

            if df.empty:
                return pd.DataFrame()

            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df["is_market_hours"] = df["timestamp"].apply(self.is_market_hours)
            market_hours_df = df[df["is_market_hours"]].copy()
            market_hours_df = market_hours_df.drop(columns=["is_market_hours"], errors="ignore")

            market_hours_df = market_hours_df.tail(MAX_BARS)
            market_hours_df = self.calculate_vwap_series(market_hours_df)
            market_hours_df = self.calculate_ema_series(market_hours_df, [8, 20, 39, 50])
            market_hours_df = self.calculate_intraday_slopes(market_hours_df, window=3)

            is_5min = timeframe.amount == 5 and timeframe.unit == TimeFrameUnit.Minute
            is_15min = timeframe.amount == 15 and timeframe.unit == TimeFrameUnit.Minute

            if is_5min:
                market_hours_df = self.calculate_rvol_5m(market_hours_df, lookback_sessions=RVOL_LOOKBACK_SESSIONS)
            elif is_15min:
                market_hours_df = self.calculate_rvol_15m(market_hours_df, lookback_sessions=RVOL_LOOKBACK_SESSIONS)

            available_columns = [
                "symbol", "timestamp", "open", "high", "low", "close", "volume", "vwap",
                "ema_8", "ema_20", "ema_39", "ema_50", "ema_39_slope", "ema_50_slope",
            ]

            if is_5min or is_15min:
                available_columns += [
                    "slot_index", "cum_volume", "rvol_slot_20", "rvol_slot_baseline_20",
                    "rvol_cum_20", "rvol_cum_baseline_20", "intraday_rvol_20",
                    "avg_daily_volume_20", "pct_of_day_typical",
                ]

            market_hours_df = market_hours_df[[c for c in available_columns if c in market_hours_df.columns]]

            if "timestamp" in market_hours_df.columns:
                if market_hours_df["timestamp"].dt.tz is not None:
                    market_hours_df["timestamp"] = market_hours_df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(
                        None)

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
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    records = df.to_dict('records')
                    all_records.extend(records)

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

                batched = [[record.get(col) for col in columns] for record in all_records]

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

    def batch_store_first_hour_data(self, data_dict: Dict[str, pd.DataFrame]):
        """Store first hour 1-minute candles in Postgres."""
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
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        # Ensure UTC and remove timezone for storage
                        if df['timestamp'].dt.tz is not None:
                            df['timestamp'] = df['timestamp'].dt.tz_convert('UTC').dt.tz_localize(None)
                        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    records = df.to_dict('records')
                    all_records.extend(records)

            if symbols_to_delete:
                placeholders = ",".join(["%s"] * len(symbols_to_delete))
                cur.execute(
                    f"DELETE FROM candles_1min_first_hour WHERE symbol IN ({placeholders})",
                    list(symbols_to_delete),
                )

            if all_records:
                # Define columns for first hour table
                columns = [
                    "symbol", "timestamp", "open", "high", "low", "close", "volume", "vwap",
                    "ema_8", "ema_20", "ema_39", "ema_50", "ema_39_slope", "ema_50_slope",
                    "slot_index", "cum_volume", "rvol_slot_20", "rvol_slot_baseline_20",
                    "rvol_cum_20", "rvol_cum_baseline_20", "ib_high", "ib_low", "ib_range"
                ]

                # Filter to only columns that exist in records
                available_cols = [c for c in columns if c in all_records[0]]
                col_list = ",".join(available_cols)
                placeholders = ",".join(["%s"] * len(available_cols))

                insert_sql = f"""
                    INSERT INTO candles_1min_first_hour ({col_list})
                    VALUES ({placeholders})
                    ON CONFLICT (symbol, timestamp) DO UPDATE SET
                    {", ".join([f"{col}=EXCLUDED.{col}" for col in available_cols if col not in ("symbol", "timestamp")])}
                """

                batched = [[record.get(col) for col in available_cols] for record in all_records]

                for i in range(0, len(batched), DB_BATCH_SIZE):
                    execute_batch(
                        cur,
                        insert_sql,
                        batched[i: i + DB_BATCH_SIZE],
                        page_size=DB_BATCH_SIZE,
                    )

            conn.commit()
            logging.info(f"Batch stored {len(all_records)} first hour 1-min records")

        except Exception as e:
            logging.error(f"Error storing first hour data: {e}")
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
            cur.execute(f"DELETE FROM daily_bars WHERE symbol IN ({placeholders})", symbols)

            columns = [
                "symbol", "timestamp", "open", "high", "low", "close", "volume",
                "vwap", "vwap_upper", "vwap_lower", "session_high", "session_low",
                "poc", "atr", "rsi_10", "momentum_10", "ema_8", "ema_20", "ema_39_of_15min",
                "ema_39", "ema_50", "ema_39_slope", "ema_50_slope",
                "adx", "adx_sma", "adx_slope", "di_plus", "di_minus",
            ]
            col_list = ",".join(columns)
            placeholders = ",".join(["%s"] * len(columns))

            insert_sql = f"""
                INSERT INTO daily_bars ({col_list})
                VALUES ({placeholders})
                ON CONFLICT (symbol, timestamp) DO UPDATE SET
                {", ".join([f"{col}=EXCLUDED.{col}" for col in columns if col not in ("symbol", "timestamp")])}
            """

            batched = [
                [
                    bar["symbol"], bar["timestamp"], bar["open"], bar["high"],
                    bar["low"], bar["close"], bar["volume"], bar.get("vwap"),
                    bar.get("vwap_upper"), bar.get("vwap_lower"), bar.get("session_high"),
                    bar.get("session_low"), bar.get("poc"), bar.get("atr"),
                    bar.get("rsi_10"), bar.get("momentum_10"), bar.get("ema_8"),
                    bar.get("ema_20"), bar.get("ema_39_of_15min"),
                    bar.get("ema_39"), bar.get("ema_50"), bar.get("ema_39_slope"), bar.get("ema_50_slope"),
                    bar.get("adx"), bar.get("adx_sma"), bar.get("adx_slope"),
                    bar.get("di_plus"), bar.get("di_minus"),
                ]
                for bar in all_records
            ]

            for i in range(0, len(batched), DB_BATCH_SIZE):
                execute_batch(cur, insert_sql, batched[i: i + DB_BATCH_SIZE], page_size=DB_BATCH_SIZE)

            conn.commit()
            logging.info(f"Batch stored {len(all_records)} daily bars")

        except Exception as e:
            logging.error(f"Error storing daily bars: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def update_daily_bars_with_ema39_of_15min(self, symbols: List[str]):
        """Set daily_bars.ema_39_of_15min to the last 15-minute ema_39 value of that day."""
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
                    SELECT symbol, timestamp :: date AS trade_date, ema_39, timestamp
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
            logging.info(f"Updated ema_39_of_15min in daily_bars for {len(symbols)} symbols")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating ema_39_of_15min: {e}")
        finally:
            cur.close()
            conn.close()

    # -----------------------------------------------------------------------
    # Orchestration & summary
    # -----------------------------------------------------------------------

    def collect_all_data(self, days_back: int = 360):
        """Main method to collect data for all tickers (full rebuild)."""
        self.start_time = time.time()
        logging.info("Starting data collection process (full rebuild)")

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

            # Process daily bars and first hour data together
            daily_bars_batch, first_hour_batch = self.process_symbol_batch(batch, days_back)
            self.batch_store_daily_bars(daily_bars_batch)
            self.batch_store_first_hour_data(first_hour_batch)

            # Fetch & store 5min candles
            data_5min = self.get_market_hours_data_batch(
                batch, TimeFrame(5, TimeFrameUnit.Minute), days_back=days_back
            )
            self.batch_store_data(data_5min, "candles_5min")

            # Fetch & store 15min candles
            data_15min = self.get_market_hours_data_batch(
                batch, TimeFrame(15, TimeFrameUnit.Minute), days_back=days_back
            )
            self.batch_store_data(data_15min, "candles_15min")

            # Backfill daily_bars.ema_39_of_15min
            self.update_daily_bars_with_ema39_of_15min(batch)

            batch_time = time.time() - batch_start_time
            logging.info(f"Batch completed in {batch_time:.2f} seconds")

        total_time = time.time() - self.start_time
        logging.info(f"Data collection completed in {total_time:.2f} seconds")
        logging.info(f"Total API calls: {self.api_call_count}")
        logging.info(f"Average time per ticker: {total_time / len(tickers):.2f} seconds")
        logging.info(f"API calls per ticker: {self.api_call_count / len(tickers):.2f}")

    def get_data_summary(self):
        """Print summary of stored data from Postgres."""
        conn = self._get_pg_conn()
        cur = conn.cursor()

        try:
            tables = ["candles_5min", "candles_15min", "candles_1min_first_hour", "daily_bars",
                      "composite_session_profiles"]

            for table in tables:
                if table == "composite_session_profiles":
                    cur.execute(f"""
                        SELECT 
                            COUNT(DISTINCT symbol) AS symbol_count,
                            COUNT(*) AS total_records,
                            MIN(session_date) AS earliest,
                            MAX(session_date) AS latest
                        FROM {table}
                    """)
                else:
                    cur.execute(f"""
                        SELECT 
                            COUNT(DISTINCT symbol) AS symbol_count,
                            COUNT(*) AS total_records,
                            MIN(timestamp) AS earliest,
                            MAX(timestamp) AS latest
                        FROM {table}
                    """)
                symbol_count, total_records, earliest, latest = cur.fetchone()

                print(f"\n{table} Summary:")
                print(f"  Symbols: {symbol_count}")
                print(f"  Total Records: {total_records}")
                print(f"  Date Range: {earliest} to {latest}")

            # First hour 1-min quality check
            cur.execute("""
                        SELECT COUNT(*)                             as total_rows,
                               COUNT(rvol_slot_20)                  as slot_rvol_populated,
                               COUNT(rvol_cum_20)                   as cum_rvol_populated,
                               COUNT(ib_high)                       as ib_populated,
                               ROUND(AVG(rvol_slot_20)::numeric, 2) as avg_slot_rvol,
                               ROUND(AVG(ib_range)::numeric, 2)     as avg_ib_range
                        FROM candles_1min_first_hour
                        WHERE timestamp > CURRENT_DATE - INTERVAL '30 days'
                        """)
            row = cur.fetchone()
            if row:
                print(f"\nFirst Hour 1-Min Quality (last 30 days):")
                print(f"  Total rows: {row[0]}")
                print(f"  Slot RVOL populated: {row[1]} ({100 * row[1] / max(row[0], 1):.1f}%)")
                print(f"  Cum RVOL populated: {row[2]} ({100 * row[2] / max(row[0], 1):.1f}%)")
                print(f"  IB metrics populated: {row[3]} ({100 * row[3] / max(row[0], 1):.1f}%)")
                print(f"  Avg Slot RVOL: {row[4]}")
                print(f"  Avg IB Range: {row[5]}")

            # RVOL quality check for 5min
            cur.execute("""
                        SELECT COUNT(*)                                                                     as total_rows,
                               COUNT(rvol_slot_20)                                                          as slot_rvol_populated,
                               COUNT(rvol_cum_20)                                                           as cum_rvol_populated,
                               COUNT(intraday_rvol_20)                                                      as intraday_rvol_populated,
                               ROUND(AVG(rvol_slot_20)::numeric, 2)                                         as avg_slot_rvol,
                               ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rvol_slot_20)::numeric, 2) as median_slot_rvol,
                               ROUND(AVG(intraday_rvol_20)::numeric, 2)                                     as avg_intraday_rvol
                        FROM candles_5min
                        WHERE timestamp > CURRENT_DATE - INTERVAL '30 days'
                        """)
            row = cur.fetchone()
            if row:
                print(f"\nRVOL Quality - candles_5min (last 30 days):")
                print(f"  Total rows: {row[0]}")
                print(f"  Slot RVOL populated: {row[1]} ({100 * row[1] / max(row[0], 1):.1f}%)")
                print(f"  Cum RVOL populated: {row[2]} ({100 * row[2] / max(row[0], 1):.1f}%)")
                print(f"  Intraday RVOL populated: {row[3]} ({100 * row[3] / max(row[0], 1):.1f}%)")
                print(f"  Avg Slot RVOL: {row[4]}")
                print(f"  Median Slot RVOL: {row[5]}")
                print(f"  Avg Intraday RVOL: {row[6]}")

            # TrendEngine indicators quality check
            cur.execute("""
                        SELECT COUNT(*)                    as total_rows,
                               COUNT(ema_39)               as ema39_populated,
                               COUNT(ema_50)               as ema50_populated,
                               COUNT(adx)                  as adx_populated,
                               ROUND(AVG(adx)::numeric, 2) as avg_adx
                        FROM daily_bars
                        WHERE timestamp > CURRENT_DATE - INTERVAL '30 days'
                        """)
            row = cur.fetchone()
            if row:
                print(f"\nTrendEngine Indicators - daily_bars (last 30 days):")
                print(f"  Total rows: {row[0]}")
                print(f"  EMA39 populated: {row[1]} ({100 * row[1] / max(row[0], 1):.1f}%)")
                print(f"  EMA50 populated: {row[2]} ({100 * row[2] / max(row[0], 1):.1f}%)")
                print(f"  ADX populated: {row[3]} ({100 * row[3] / max(row[0], 1):.1f}%)")
                print(f"  Avg ADX: {row[4]}")

        finally:
            cur.close()
            conn.close()


def main():
    """Main function to run the data collector."""
    API_KEY = os.getenv("APCA_API_KEY_ID")
    SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

    if not API_KEY or not SECRET_KEY:
        print("Error: Please set APCA_API_KEY_ID and APCA_API_SECRET_KEY environment variables")
        return

    try:
        collector = AlpacaDataCollectorOptimized(API_KEY, SECRET_KEY)

        start_time = time.time()
        collector.collect_all_data(days_back=360)

        collector.get_data_summary()

        total_time = time.time() - start_time
        print(f"\n✅ Total execution time: {total_time:.2f} seconds ({total_time / 60:.2f} minutes)")

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise


if __name__ == "__main__":
    main()