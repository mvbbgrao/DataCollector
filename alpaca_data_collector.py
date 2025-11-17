#!/usr/bin/env python3
"""
Alpaca Candlestick Data Collector - Ultra Optimized Version
Fetches 5min and 15min candlestick data and stores in local SQLite database.
Optimized for processing large numbers of symbols quickly using parallel processing and batch operations.
"""

import sqlite3
from alpaca.data import TimeFrameUnit
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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

# Load environment variables
load_dotenv()
# Configuration
DATABASE_NAME = "E:/database/market_data.db"
TICKERS_FILE = "ticker_list.txt"
SPECIAL_TICKERS_FILE = "special_ticker_list.txt"
MAX_BARS = 600

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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_collector_optimized.log'),
        logging.StreamHandler()
    ]
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

    def setup_database(self):
        """Create database tables if they don't exist."""
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        # Create table for 5min data
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS candles_5min
                       (
                           id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           symbol
                           TEXT
                           NOT
                           NULL,
                           timestamp
                           DATETIME
                           NOT
                           NULL,
                           open
                           REAL
                           NOT
                           NULL,
                           high
                           REAL
                           NOT
                           NULL,
                           low
                           REAL
                           NOT
                           NULL,
                           close
                           REAL
                           NOT
                           NULL,
                           volume
                           INTEGER
                           NOT
                           NULL,
                           vwap
                           REAL,
                           ema_8
                           REAL,
                           ema_20
                           REAL,
                           ema_39
                           REAL,
                           created_at
                           DATETIME
                           DEFAULT
                           CURRENT_TIMESTAMP,
                           UNIQUE
                       (
                           symbol,
                           timestamp
                       )
                           )
                       ''')

        # Create table for 15min data
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS candles_15min
                       (
                           id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           symbol
                           TEXT
                           NOT
                           NULL,
                           timestamp
                           DATETIME
                           NOT
                           NULL,
                           open
                           REAL
                           NOT
                           NULL,
                           high
                           REAL
                           NOT
                           NULL,
                           low
                           REAL
                           NOT
                           NULL,
                           close
                           REAL
                           NOT
                           NULL,
                           volume
                           INTEGER
                           NOT
                           NULL,
                           vwap
                           REAL,
                           ema_8
                           REAL,
                           ema_20
                           REAL,
                           ema_39
                           REAL,
                           created_at
                           DATETIME
                           DEFAULT
                           CURRENT_TIMESTAMP,
                           UNIQUE
                       (
                           symbol,
                           timestamp
                       )
                           )
                       ''')

        # Add daily bars table with all indicators
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS daily_bars
                       (
                           id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           symbol
                           TEXT
                           NOT
                           NULL,
                           timestamp
                           DATE
                           NOT
                           NULL,
                           open
                           REAL
                           NOT
                           NULL,
                           high
                           REAL
                           NOT
                           NULL,
                           low
                           REAL
                           NOT
                           NULL,
                           close
                           REAL
                           NOT
                           NULL,
                           volume
                           INTEGER
                           NOT
                           NULL,
                           vwap
                           REAL,
                           vwap_upper
                           REAL,
                           vwap_lower
                           REAL,
                           session_high
                           REAL,
                           session_low
                           REAL,
                           poc
                           REAL,
                           atr
                           REAL,
                           rsi_10
                           REAL,
                           momentum_10
                           REAL,
                           created_at
                           DATETIME
                           DEFAULT
                           CURRENT_TIMESTAMP,
                           UNIQUE
                       (
                           symbol,
                           timestamp
                       )
                           )
                       ''')

        # Add new columns to daily_bars if not exist (for upgrades)
        daily_columns = ['session_high', 'session_low', 'poc', 'atr', 'rsi_10', 'momentum_10', 'vwap_upper',
                         'vwap_lower']
        for column in daily_columns:
            try:
                cursor.execute(f'ALTER TABLE daily_bars ADD COLUMN {column} REAL')
            except sqlite3.OperationalError:
                pass  # Column already exists

        # Create indexes for better query performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_5min_symbol_timestamp ON candles_5min(symbol, timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_15min_symbol_timestamp ON candles_15min(symbol, timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_symbol_timestamp ON daily_bars(symbol, timestamp)')

        # Enable WAL mode for better concurrency
        cursor.execute('PRAGMA journal_mode=WAL')
        cursor.execute('PRAGMA synchronous=NORMAL')

        conn.commit()
        conn.close()
        logging.info("Database setup completed with optimizations")

    def load_tickers(self) -> List[str]:
        """Load tickers from the text file."""
        if not os.path.exists(TICKERS_FILE):
            logging.error(f"Tickers file {TICKERS_FILE} not found")
            return []

        with open(TICKERS_FILE, 'r') as f:
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

    def calculate_rsi(self, df: pd.DataFrame, period: int = 10) -> pd.DataFrame:
        """Calculate RSI (Relative Strength Index) for given period."""
        if df.empty or len(df) < period + 1:
            df['rsi_10'] = np.nan
            return df

        df = df.copy()
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Vectorized calculation for better performance
        df['price_change'] = df['close'].diff()
        df['gain'] = np.where(df['price_change'] > 0, df['price_change'], 0)
        df['loss'] = np.where(df['price_change'] < 0, -df['price_change'], 0)

        # Use pandas rolling for initial averages
        avg_gain = df['gain'].rolling(window=period).mean()
        avg_loss = df['loss'].rolling(window=period).mean()

        # Calculate RS and RSI
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df['rsi_10'] = 100 - (100 / (1 + rs))
        df['rsi_10'] = df['rsi_10'].round(2)

        # Clean up temporary columns
        df = df.drop(['price_change', 'gain', 'loss'], axis=1)

        return df

    def calculate_momentum(self, df: pd.DataFrame, period: int = 10) -> pd.DataFrame:
        """Calculate Momentum indicator."""
        if df.empty or len(df) < period + 1:
            df['momentum_10'] = np.nan
            return df

        df = df.copy()
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Vectorized calculation
        df['momentum_10'] = (df['close'] / df['close'].shift(period)) * 100
        df['momentum_10'] = df['momentum_10'].round(2)

        return df

    def calculate_vwap_series(self, df: pd.DataFrame, include_bands: bool = False,
                              multiplier: float = 1.5) -> pd.DataFrame:
        """Calculate VWAP for each bar with optional bands - optimized version."""
        if df.empty:
            return df

        df = df.copy()
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Ensure timestamp is datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Vectorized typical price calculation
        df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
        df['pv'] = df['typical_price'] * df['volume']

        # Group by date for daily VWAP calculation
        if 'date' not in df.columns:
            df['date'] = df['timestamp'].dt.date

        # Cumulative calculations within each day
        df['cum_pv'] = df.groupby('date')['pv'].cumsum()
        df['cum_volume'] = df.groupby('date')['volume'].cumsum()

        # Handle division by zero
        df['vwap'] = 0.0
        mask = df['cum_volume'] > 0
        df.loc[mask, 'vwap'] = (df.loc[mask, 'cum_pv'] / df.loc[mask, 'cum_volume']).round(2)

        if include_bands:
            # Calculate bands using vectorized operations
            df['vwap_upper'] = df['vwap']
            df['vwap_lower'] = df['vwap']

            for date in df['date'].unique():
                date_mask = df['date'] == date
                date_df = df[date_mask]

                if len(date_df) == 0:
                    continue

                # Calculate rolling standard deviation
                vwap_values = date_df['vwap'].values
                typical_prices = date_df['typical_price'].values
                volumes = date_df['volume'].values

                # Calculate weighted standard deviation for each point
                upper_band = []
                lower_band = []

                for i in range(len(date_df)):
                    volume_sum = volumes[:i + 1].sum()
                    if volume_sum > 0:
                        weights = volumes[:i + 1] / volume_sum
                        weighted_mean = vwap_values[i]
                        variance = np.sum(weights * (typical_prices[:i + 1] - weighted_mean) ** 2)
                        std_dev = np.sqrt(variance) if variance > 0 else 0

                        upper_band.append(weighted_mean + multiplier * std_dev)
                        lower_band.append(weighted_mean - multiplier * std_dev)
                    else:
                        upper_band.append(vwap_values[i] if i < len(vwap_values) else 0)
                        lower_band.append(vwap_values[i] if i < len(vwap_values) else 0)

                # Assign calculated bands back to dataframe
                df.loc[date_mask, 'vwap_upper'] = np.round(upper_band, 2)
                df.loc[date_mask, 'vwap_lower'] = np.round(lower_band, 2)

        # Clean up temporary columns
        columns_to_drop = ['typical_price', 'pv', 'cum_pv', 'cum_volume']
        # Don't drop 'date' as it might be needed later
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], axis=1)

        return df

    def calculate_ema_series(self, df: pd.DataFrame, periods: List[int]) -> pd.DataFrame:
        """Calculate Exponential Moving Averages for given periods."""
        if df.empty:
            return df

        df = df.copy()
        df = df.sort_values('timestamp').reset_index(drop=True)

        for period in periods:
            df[f'ema_{period}'] = df['close'].ewm(span=period, adjust=False).mean().round(3)

        return df

    def fetch_and_process_session_data(self, symbol: str, dates: List[str]) -> Dict[str, SessionStats]:
        """Fetch and process multiple days of session data at once."""
        if not dates:
            return {}

        # Convert dates to datetime range
        dates_dt = [datetime.strptime(d, "%Y-%m-%d") for d in dates]
        start_date = min(dates_dt)
        end_date = max(dates_dt) + timedelta(days=1)

        # Fetch all 1-minute data in one API call
        est = self.est_tz
        session_start = est.localize(datetime.combine(start_date, datetime.min.time()).replace(hour=9, minute=30))
        session_end = est.localize(datetime.combine(end_date, datetime.min.time()).replace(hour=15, minute=59))

        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=TimeFrame(1, TimeFrameUnit.Minute),
            start=session_start.isoformat(),
            end=session_end.isoformat()
        )

        try:
            self.api_call_count += 1
            bars = self.client.get_stock_bars(request)
            df = bars.df.reset_index()

            if df.empty:
                return {date: None for date in dates}

            # Ensure timestamp is datetime and add date column for grouping
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df['date'] = df['timestamp'].dt.date
            else:
                logging.error(f"No timestamp column found for {symbol}")
                return {date: None for date in dates}

            # Calculate VWAP with bands for all data at once
            df = self.calculate_vwap_series(df, include_bands=True)

            results = {}

            # Process each day's data
            for date_str in dates:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                day_df = df[df['date'] == date_obj].copy()

                if day_df.empty:
                    results[date_str] = None
                    continue

                # Calculate session stats
                highest_high = day_df['high'].max()

                # Calculate volume profile
                try:
                    volume_profile_metrics = self.calculate_volume_profile_optimized(day_df)
                    session_high = volume_profile_metrics.value_area_high
                    session_low = volume_profile_metrics.value_area_low
                    poc = volume_profile_metrics.poc_price
                except Exception as e:
                    logging.warning(f"Could not calculate volume profile for {symbol} on {date_str}: {e}")
                    session_high = day_df['high'].max()
                    session_low = day_df['low'].min()
                    poc = day_df.loc[day_df['volume'].idxmax(), 'close'] if not day_df.empty else None

                # Get VWAP bands from the last bar of the day
                vwap_upper = day_df['vwap_upper'].iloc[-1] if 'vwap_upper' in day_df.columns and len(
                    day_df) > 0 else None
                vwap_lower = day_df['vwap_lower'].iloc[-1] if 'vwap_lower' in day_df.columns and len(
                    day_df) > 0 else None

                results[date_str] = SessionStats(
                    highest_high=highest_high,
                    session_high=session_high,
                    session_low=session_low,
                    poc=poc,
                    vwap_upper=vwap_upper,
                    vwap_lower=vwap_lower
                )

            return results

        except Exception as e:
            logging.error(f"Failed to fetch session data for {symbol}: {e}")
            return {date: None for date in dates}

    def calculate_volume_profile_optimized(self, df: pd.DataFrame, price_levels: int = 70) -> VolumeProfileMetrics:
        """Optimized volume profile calculation using numpy operations."""
        if df.empty:
            raise ValueError("DataFrame is empty")

        # Get price range
        high_price = df['high'].max()
        low_price = df['low'].min()
        price_range = high_price - low_price

        if price_range == 0:
            raise ValueError("No price movement in the data")

        # Create price bins using numpy
        price_bins = np.linspace(low_price, high_price, price_levels + 1)

        # Vectorized volume distribution
        volume_at_price = np.zeros(price_levels)

        for _, row in df.iterrows():
            bar_low = row['low']
            bar_high = row['high']
            bar_volume = row['volume']

            if bar_volume == 0:
                continue

            # Find affected bins using numpy operations
            affected_bins = np.where((price_bins[:-1] <= bar_high) & (price_bins[1:] >= bar_low))[0]

            if len(affected_bins) > 0:
                volume_per_bin = bar_volume / len(affected_bins)
                volume_at_price[affected_bins] += volume_per_bin

        # Find POC
        poc_index = np.argmax(volume_at_price)
        poc_price = (price_bins[poc_index] + price_bins[poc_index + 1]) / 2

        # Calculate value area (70% of volume)
        total_volume = volume_at_price.sum()
        value_area_volume = total_volume * 0.7

        # Expand from POC
        current_volume = volume_at_price[poc_index]
        low_index = high_index = poc_index

        while current_volume < value_area_volume:
            expand_low = low_index > 0
            expand_high = high_index < price_levels - 1

            if not expand_low and not expand_high:
                break

            low_vol = volume_at_price[low_index - 1] if expand_low else 0
            high_vol = volume_at_price[high_index + 1] if expand_high else 0

            if expand_low and (not expand_high or low_vol >= high_vol):
                low_index -= 1
                current_volume += volume_at_price[low_index]
            elif expand_high:
                high_index += 1
                current_volume += volume_at_price[high_index]

        value_area_high = (price_bins[high_index] + price_bins[high_index + 1]) / 2
        value_area_low = (price_bins[low_index] + price_bins[low_index + 1]) / 2

        # Create profile levels (simplified for performance)
        profile_levels = []
        for i in range(price_levels):
            if volume_at_price[i] > 0:
                price_level = (price_bins[i] + price_bins[i + 1]) / 2
                profile_levels.append(VolumeProfileLevel(
                    price_level=price_level,
                    volume=int(volume_at_price[i]),
                    percentage=(volume_at_price[i] / total_volume) * 100 if total_volume > 0 else 0,
                    is_poc=(i == poc_index),
                    is_value_area=(low_index <= i <= high_index)
                ))

        return VolumeProfileMetrics(
            poc_price=round(poc_price, 2),
            value_area_high=round(value_area_high, 2),
            value_area_low=round(value_area_low, 2),
            total_volume=int(total_volume),
            price_levels=profile_levels
        )

    def process_symbol_batch(self, symbols: List[str], days_back: int = 40) -> Dict[str, List[dict]]:
        """Process a batch of symbols in parallel."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        # Fetch daily bars for all symbols in one request (if API supports it)
        all_results = {}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all symbol processing tasks
            future_to_symbol = {
                executor.submit(self.process_single_symbol, symbol, start_date, end_date): symbol
                for symbol in symbols
            }

            # Collect results as they complete
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

    def process_single_symbol(self, symbol: str, start_date: datetime, end_date: datetime) -> List[dict]:
        """Process a single symbol - fetch daily bars and session stats."""
        try:
            # Fetch daily bars
            request = StockBarsRequest(
                symbol_or_symbols=[symbol],
                timeframe=TimeFrame(1, TimeFrameUnit.Day),
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                feed="iex"
            )

            self.api_call_count += 1
            bars = self.client.get_stock_bars(request)
            df = bars.df.reset_index()

            if df.empty:
                return []

            # Ensure timestamp is datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # Calculate technical indicators
            df = df.sort_values('timestamp').reset_index(drop=True)
            df = self.calculate_daily_indicators_optimized(df)

            # Get all dates that need session stats
            dates_to_fetch = [row['timestamp'].date().isoformat() for _, row in df.iterrows()]

            # Fetch all session stats in one call
            session_stats = self.fetch_and_process_session_data(symbol, dates_to_fetch)

            # Combine daily bars with session stats
            result = []
            for _, row in df.iterrows():
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
                    "volume": int(row["volume"]) if pd.notna(row["volume"]) else 0,
                    "vwap": float(row.get("vwap")) if pd.notna(row.get("vwap")) else None,
                    "vwap_upper": float(stats.vwap_upper) if stats and stats.vwap_upper else None,
                    "vwap_lower": float(stats.vwap_lower) if stats and stats.vwap_lower else None,
                    "session_high": float(stats.session_high) if stats and stats.session_high else None,
                    "session_low": float(stats.session_low) if stats and stats.session_low else None,
                    "poc": float(stats.poc) if stats and stats.poc else None,
                    "atr": float(row.get("atr")) if pd.notna(row.get("atr")) else None,
                    "rsi_10": float(row.get("rsi_10")) if pd.notna(row.get("rsi_10")) else None,
                    "momentum_10": float(row.get("momentum_10")) if pd.notna(row.get("momentum_10")) else None
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

        # ATR calculation
        df['high_low'] = df['high'] - df['low']
        df['high_pc'] = abs(df['high'] - df['close'].shift(1))
        df['low_pc'] = abs(df['low'] - df['close'].shift(1))
        df['true_range'] = df[['high_low', 'high_pc', 'low_pc']].max(axis=1)

        # Use exponential moving average for ATR
        df['atr'] = df['true_range'].ewm(span=10, adjust=False).mean()

        # RSI calculation
        df = self.calculate_rsi(df, period=10)

        # Momentum calculation
        df = self.calculate_momentum(df, period=10)

        # Clean up temporary columns
        df = df.drop(['high_low', 'high_pc', 'low_pc', 'true_range'], axis=1, errors='ignore')

        return df

    def get_market_hours_data_batch(self, symbols: List[str], timeframe: TimeFrame, days_back: int = 10) -> Dict[
        str, pd.DataFrame]:
        """Fetch market hours data for multiple symbols in parallel."""
        results = {}

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

    def get_market_hours_data_single(self, symbol: str, timeframe: TimeFrame, days_back: int = 10) -> pd.DataFrame:
        """Fetch market hours data for a single symbol."""
        end_date = datetime.now(self.est_tz)
        start_date = end_date - timedelta(days=days_back)

        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=timeframe,
            start=start_date.isoformat(),
            end=end_date.isoformat()
        )

        try:
            self.api_call_count += 1
            bars = self.client.get_stock_bars(request)
            df = bars.df.reset_index()

            if df.empty:
                return pd.DataFrame()

            # Filter for market hours only
            df['is_market_hours'] = df['timestamp'].apply(self.is_market_hours)
            market_hours_df = df[df['is_market_hours']].copy()

            # Keep only the last MAX_BARS
            market_hours_df = market_hours_df.tail(MAX_BARS)

            # Calculate VWAP and EMAs
            market_hours_df = self.calculate_vwap_series(market_hours_df)
            market_hours_df = self.calculate_ema_series(market_hours_df, [8, 20, 39])

            # Clean up columns
            available_columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume',
                                 'vwap', 'ema_8', 'ema_20', 'ema_39']
            market_hours_df = market_hours_df[available_columns]

            return market_hours_df

        except Exception as e:
            logging.error(f"Error fetching data for {symbol}: {str(e)}")
            return pd.DataFrame()

    def batch_store_data(self, data_dict: Dict[str, pd.DataFrame], table_name: str):
        """Store multiple dataframes in batch operations."""
        if not data_dict:
            return

        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        try:
            # Prepare all data for batch insert
            all_records = []
            symbols_to_delete = set()

            for symbol, df in data_dict.items():
                if not df.empty:
                    symbols_to_delete.add(symbol)
                    # Convert timestamp to string format for SQLite
                    df = df.copy()
                    if 'timestamp' in df.columns:
                        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    records = df.to_dict('records')
                    all_records.extend(records)

            # Delete existing data for all symbols at once
            if symbols_to_delete:
                placeholders = ','.join(['?' for _ in symbols_to_delete])
                cursor.execute(f'DELETE FROM {table_name} WHERE symbol IN ({placeholders})',
                               list(symbols_to_delete))

            # Batch insert all records
            if all_records:
                # Get column names from first record
                columns = list(all_records[0].keys())
                placeholders = ','.join(['?' for _ in columns])

                # Insert in batches
                for i in range(0, len(all_records), DB_BATCH_SIZE):
                    batch = all_records[i:i + DB_BATCH_SIZE]
                    values = [[record.get(col) for col in columns] for record in batch]

                    cursor.executemany(
                        f'INSERT OR REPLACE INTO {table_name} ({",".join(columns)}) VALUES ({placeholders})',
                        values
                    )

            conn.commit()
            logging.info(f"Batch stored {len(all_records)} records in {table_name}")

        except Exception as e:
            logging.error(f"Error in batch store: {e}")
            conn.rollback()
        finally:
            conn.close()

    def batch_store_daily_bars(self, all_bars: Dict[str, List[dict]]):
        """Store all daily bars in batch operations."""
        if not all_bars:
            return

        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        try:
            # Prepare all records
            all_records = []
            for symbol, bars in all_bars.items():
                all_records.extend(bars)

            if not all_records:
                return

            # Clear existing data for all symbols at once
            symbols = list(all_bars.keys())
            placeholders = ','.join(['?' for _ in symbols])
            cursor.execute(f'DELETE FROM daily_bars WHERE symbol IN ({placeholders})', symbols)

            # Batch insert
            for i in range(0, len(all_records), DB_BATCH_SIZE):
                batch = all_records[i:i + DB_BATCH_SIZE]

                cursor.executemany('''
                                   INSERT INTO daily_bars
                                   (symbol, timestamp, open, high, low, close, volume, vwap, vwap_upper, vwap_lower,
                                    session_high, session_low, poc, atr, rsi_10, momentum_10)
                                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                   ''', [(
                    bar["symbol"], bar["timestamp"], bar["open"], bar["high"], bar["low"],
                    bar["close"], bar["volume"], bar.get("vwap"), bar.get("vwap_upper"), bar.get("vwap_lower"),
                    bar.get("session_high"), bar.get("session_low"), bar.get("poc"),
                    bar.get("atr"), bar.get("rsi_10"), bar.get("momentum_10")
                ) for bar in batch])

            conn.commit()
            logging.info(f"Batch stored {len(all_records)} daily bars")

        except Exception as e:
            logging.error(f"Error storing daily bars: {e}")
            conn.rollback()
        finally:
            conn.close()

    def collect_all_data_optimized(self, incremental: bool = False):
        """Main optimized method to collect data for all tickers."""
        self.start_time = time.time()
        logging.info("Starting optimized data collection process")

        # Setup database
        self.setup_database()

        # Load tickers
        tickers = self.load_tickers()
        if not tickers:
            logging.error("No tickers to process")
            return

        logging.info(f"Processing {len(tickers)} tickers in batches of {BATCH_SIZE}")

        # Process tickers in batches
        for i in range(0, len(tickers), BATCH_SIZE):
            batch = tickers[i:i + BATCH_SIZE]
            batch_start_time = time.time()

            logging.info(f"Processing batch {i // BATCH_SIZE + 1}/{(len(tickers) - 1) // BATCH_SIZE + 1}: {batch}")

            # Process daily bars for all symbols in batch
            days_back = 40 if not incremental else 1
            daily_bars_batch = self.process_symbol_batch(batch, days_back)

            # Store daily bars in batch
            self.batch_store_daily_bars(daily_bars_batch)

            # Get 5-minute data for all symbols in batch
            data_5min = self.get_market_hours_data_batch(batch, TimeFrame(5, TimeFrameUnit.Minute))
            self.batch_store_data(data_5min, 'candles_5min')

            # Get 15-minute data for all symbols in batch
            data_15min = self.get_market_hours_data_batch(batch, TimeFrame(15, TimeFrameUnit.Minute))
            self.batch_store_data(data_15min, 'candles_15min')

            batch_time = time.time() - batch_start_time
            logging.info(f"Batch completed in {batch_time:.2f} seconds")

        # Calculate and display performance metrics
        total_time = time.time() - self.start_time
        logging.info(f"Data collection completed in {total_time:.2f} seconds")
        logging.info(f"Total API calls: {self.api_call_count}")
        logging.info(f"Average time per ticker: {total_time / len(tickers):.2f} seconds")
        logging.info(f"API calls per ticker: {self.api_call_count / len(tickers):.2f}")

    def get_data_summary(self):
        """Print summary of stored data."""
        conn = sqlite3.connect(DATABASE_NAME)

        # Get summary for each table
        tables = ['candles_5min', 'candles_15min', 'daily_bars']

        for table in tables:
            cursor = conn.execute(f'''
                SELECT 
                    COUNT(DISTINCT symbol) as symbol_count,
                    COUNT(*) as total_records,
                    MIN(timestamp) as earliest,
                    MAX(timestamp) as latest
                FROM {table}
            ''')

            result = cursor.fetchone()
            print(f"\n{table} Summary:")
            print(f"  Symbols: {result[0]}")
            print(f"  Total Records: {result[1]}")
            print(f"  Date Range: {result[2]} to {result[3]}")

        conn.close()


def main():
    """Main function to run the optimized data collector."""
    # Load environment variables
    API_KEY = os.getenv('APCA_API_KEY_ID')
    SECRET_KEY = os.getenv('APCA_API_SECRET_KEY')

    if not API_KEY or not SECRET_KEY:
        print("Error: Please set APCA_API_KEY_ID and APCA_API_SECRET_KEY environment variables")
        return

    try:
        collector = AlpacaDataCollectorOptimized(API_KEY, SECRET_KEY)

        # Run the optimized collection
        start_time = time.time()
        collector.collect_all_data_optimized(incremental=False)

        # Display summary
        collector.get_data_summary()

        total_time = time.time() - start_time
        print(f"\n✅ Total execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise


if __name__ == "__main__":
    main()