#!/usr/bin/env python3
"""
Enhanced Alpaca Candlestick Data Collector
Fetches 5min and 15min candlestick data and stores in local SQLite database.
Supports both full and incremental sync modes.
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

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_collector.log'),
        logging.StreamHandler()
    ]
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


class AlpacaDataCollector:
    def __init__(self, api_key: str, secret_key: str):
        """Initialize the Alpaca data collector."""
        self.client = StockHistoricalDataClient(api_key, secret_key)
        self.pst_tz = pytz.timezone('US/Pacific')
        self.est_tz = pytz.timezone('US/Eastern')

    def setup_database(self):
        """Create database tables if they don't exist."""
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        # Create table for 5min data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS candles_5min (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                vwap REAL,
                ema_8 REAL,
                ema_20 REAL,
                ema_39 REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, timestamp)
            )
        ''')

        # Create table for 15min data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS candles_15min (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                vwap REAL,
                ema_8 REAL,
                ema_20 REAL,
                ema_39 REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, timestamp)
            )
        ''')

        # Add daily bars table with ATR column
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_bars (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp DATE NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                vwap REAL,
                session_high REAL,
                session_low REAL,
                poc REAL,
                atr REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, timestamp)
            )
        ''')

        # Add columns if not exist (for upgrades)
        for column in ['session_high', 'session_low', 'poc', 'atr']:
            try:
                cursor.execute(f'ALTER TABLE daily_bars ADD COLUMN {column} REAL')
            except sqlite3.OperationalError:
                pass

        # Add columns to existing tables if they don't exist
        for column in ['vwap', 'ema_8', 'ema_20', 'ema_39']:
            try:
                cursor.execute(f'ALTER TABLE candles_5min ADD COLUMN {column} REAL')
            except sqlite3.OperationalError:
                pass

            try:
                cursor.execute(f'ALTER TABLE candles_15min ADD COLUMN {column} REAL')
            except sqlite3.OperationalError:
                pass

        # Create indexes for better query performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_5min_symbol_timestamp ON candles_5min(symbol, timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_15min_symbol_timestamp ON candles_15min(symbol, timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_symbol_timestamp ON daily_bars(symbol, timestamp)')

        conn.commit()
        conn.close()
        logging.info("Database setup completed")

    def load_tickers(self) -> List[str]:
        """Load tickers from the text file."""
        if not os.path.exists(TICKERS_FILE):
            logging.error(f"Tickers file {TICKERS_FILE} not found")
            return []

        with open(TICKERS_FILE, 'r') as f:
            tickers = [line.strip().upper() for line in f if line.strip()]

        logging.info(f"Loaded {len(tickers)} tickers: {tickers}")
        return tickers

    def get_last_timestamp(self, symbol: str, table_name: str) -> Optional[datetime]:
        """Get the most recent timestamp for a symbol in a table."""
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        cursor.execute(f'''
            SELECT MAX(timestamp) 
            FROM {table_name} 
            WHERE symbol = ?
        ''', (symbol,))

        result = cursor.fetchone()
        conn.close()

        if result and result[0]:
            # Parse the timestamp string to datetime
            if isinstance(result[0], str):
                # Handle both datetime and date formats
                try:
                    return datetime.fromisoformat(result[0].replace(' ', 'T'))
                except:
                    return datetime.strptime(result[0], '%Y-%m-%d')
            return result[0]
        return None

    def get_existing_data(self, symbol: str, table_name: str,
                          start_date: Optional[datetime] = None) -> pd.DataFrame:
        """Retrieve existing data from database for a symbol."""
        conn = sqlite3.connect(DATABASE_NAME)

        query = f'''
            SELECT * FROM {table_name}
            WHERE symbol = ?
        '''
        params = [symbol]

        if start_date:
            query += ' AND timestamp >= ?'
            params.append(start_date.isoformat())

        query += ' ORDER BY timestamp'

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df

    def is_market_hours(self, timestamp: datetime) -> bool:
        """Check if timestamp is within market hours."""
        if timestamp.tzinfo is None:
            timestamp = self.est_tz.localize(timestamp)

        pst_time = timestamp.astimezone(self.pst_tz)
        hour_decimal = pst_time.hour + pst_time.minute / 60.0

        return MARKET_START_PST <= hour_decimal <= MARKET_END_PST

    def get_market_hours_data_incremental(self, symbol: str, timeframe: TimeFrame,
                                          table_name: str, days_back: int = 10) -> pd.DataFrame:
        """Fetch candlestick data incrementally based on existing database data."""

        # Get the last timestamp from database
        last_timestamp = self.get_last_timestamp(symbol, table_name)

        # Determine start date for fetching new data
        if last_timestamp:
            # Start from the last timestamp (will handle duplicates later)
            start_date = last_timestamp
            logging.info(f"Incremental sync for {symbol} {timeframe}: Last timestamp in DB is {last_timestamp}")
        else:
            # No existing data, fetch from days_back
            start_date = datetime.now(self.est_tz) - timedelta(days=days_back)
            logging.info(f"No existing data for {symbol} {timeframe}, fetching last {days_back} days")

        end_date = datetime.now(self.est_tz)

        # If last timestamp is very recent (within current bar period), skip
        if last_timestamp:
            time_diff = end_date.replace(tzinfo=None) - last_timestamp.replace(tzinfo=None)
            if timeframe.amount == 5 and time_diff < timedelta(minutes=5):
                logging.info(f"Data for {symbol} {timeframe} is up to date")
                return pd.DataFrame()
            elif timeframe.amount == 15 and time_diff < timedelta(minutes=15):
                logging.info(f"Data for {symbol} {timeframe} is up to date")
                return pd.DataFrame()

        # Fetch new data from Alpaca
        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=timeframe,
            start=start_date.isoformat() if hasattr(start_date, 'isoformat') else start_date,
            end=end_date.isoformat()
        )

        try:
            bars = self.client.get_stock_bars(request)
            df_new = bars.df.reset_index()

            if df_new.empty:
                logging.warning(f"No new data received for {symbol} {timeframe}")
                return pd.DataFrame()

            # Filter for market hours
            df_new['is_market_hours'] = df_new['timestamp'].apply(self.is_market_hours)
            df_new = df_new[df_new['is_market_hours']].copy()

            # Remove duplicates (keep the new data)
            if last_timestamp:
                df_new = df_new[df_new['timestamp'] > last_timestamp]

            if df_new.empty:
                logging.info(f"No new market hours data for {symbol} {timeframe}")
                return pd.DataFrame()

            # Get existing data for EMA calculation continuity
            lookback_periods = max([8, 20, 39]) * 2  # Get enough historical data
            existing_df = self.get_existing_data(symbol, table_name)

            if not existing_df.empty:
                # Limit existing data to recent records for calculation
                existing_df = existing_df.tail(lookback_periods)

                # Combine existing and new data for calculation
                combined_df = pd.concat([existing_df, df_new], ignore_index=True)
                combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)

                # Calculate indicators on combined data
                combined_df = self.calculate_vwap_series(combined_df)
                combined_df = self.calculate_ema_series(combined_df, [8, 20, 39])

                # Extract only the new records with calculated indicators
                new_records_start_idx = len(existing_df)
                df_new = combined_df.iloc[new_records_start_idx:].copy()
            else:
                # No existing data, calculate from scratch
                df_new = self.calculate_vwap_series(df_new)
                df_new = self.calculate_ema_series(df_new, [8, 20, 39])

            # Keep only required columns
            available_columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close',
                                 'volume', 'vwap', 'ema_8', 'ema_20', 'ema_39']
            df_new = df_new[available_columns]

            # Limit to MAX_BARS if needed (for full sync)
            if not last_timestamp and len(df_new) > MAX_BARS:
                df_new = df_new.tail(MAX_BARS)

            logging.info(f"Retrieved {len(df_new)} new market hours bars for {symbol} ({timeframe})")
            return df_new

        except Exception as e:
            logging.error(f"Error fetching incremental data for {symbol}: {str(e)}")
            return pd.DataFrame()

    def get_market_hours_data(self, symbol: str, timeframe: TimeFrame, days_back: int = 10) -> pd.DataFrame:
        """Fetch candlestick data and filter for market hours only (full sync)."""
        end_date = datetime.now(self.est_tz)
        start_date = end_date - timedelta(days=days_back)

        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=timeframe,
            start=start_date.isoformat(),
            end=end_date.isoformat()
        )

        try:
            bars = self.client.get_stock_bars(request)
            df = bars.df.reset_index()

            if df.empty:
                logging.warning(f"No data received for {symbol}")
                return pd.DataFrame()

            # Filter for market hours only
            df['is_market_hours'] = df['timestamp'].apply(self.is_market_hours)
            market_hours_df = df[df['is_market_hours']].copy()

            # Keep only the last MAX_BARS
            market_hours_df = market_hours_df.tail(MAX_BARS)

            # Calculate VWAP for each bar
            market_hours_df = self.calculate_vwap_series(market_hours_df)

            # Calculate EMAs
            market_hours_df = self.calculate_ema_series(market_hours_df, [8, 20, 39])

            # Clean up columns
            available_columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close',
                                 'volume', 'vwap', 'ema_8', 'ema_20', 'ema_39']
            market_hours_df = market_hours_df[available_columns]

            logging.info(f"Retrieved {len(market_hours_df)} market hours bars for {symbol} ({timeframe})")
            return market_hours_df

        except Exception as e:
            logging.error(f"Error fetching data for {symbol}: {str(e)}")
            return pd.DataFrame()

    def calculate_ema_series(self, df: pd.DataFrame, periods: List[int]) -> pd.DataFrame:
        """Calculate Exponential Moving Averages for given periods."""
        if df.empty:
            return df

        df = df.copy()
        df = df.sort_values('timestamp').reset_index(drop=True)

        for period in periods:
            df[f'ema_{period}'] = df['close'].ewm(span=period, adjust=False).mean()
            df[f'ema_{period}'] = df[f'ema_{period}'].round(3)

        return df

    def calculate_vwap_series(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate VWAP for each bar starting from the first bar of each day."""
        if df.empty:
            return df

        df = df.copy()
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Convert timestamp to date for grouping by day
        df['date'] = pd.to_datetime(df['timestamp']).dt.date
        vwap_values = []

        # Group by date and calculate VWAP for each day separately
        for date, group in df.groupby('date'):
            cumulative_pv = 0
            cumulative_volume = 0

            group = group.sort_values('timestamp').reset_index(drop=True)

            for index, row in group.iterrows():
                typical_price = (row['high'] + row['low'] + row['close']) / 3
                volume = row['volume']

                cumulative_pv += typical_price * volume
                cumulative_volume += volume

                vwap = round(cumulative_pv / cumulative_volume, 2) if cumulative_volume > 0 else 0.00
                vwap_values.append(vwap)

        df = df.sort_values('timestamp').reset_index(drop=True)
        df['vwap'] = vwap_values
        df = df.drop('date', axis=1)

        return df

    def calculate_atr(self, symbol: str, df: pd.DataFrame, period: int = 10):
        """Calculate ATR (Average True Range) for a symbol using historical daily data."""
        try:
            if df.empty or len(df) < period + 1:
                logging.warning(f"Insufficient data for ATR calculation for {symbol}")
                return None

            df = df.sort_values('timestamp').reset_index(drop=True)

            # Calculate True Range
            df['prev_close'] = df['close'].shift(1)
            df['high_low'] = df['high'] - df['low']
            df['high_pc'] = abs(df['high'] - df['prev_close'])
            df['low_pc'] = abs(df['low'] - df['prev_close'])
            df['true_range'] = df[['high_low', 'high_pc', 'low_pc']].max(axis=1)

            # Calculate ATR using Wilder's method
            first_atr = df['true_range'].iloc[1:period + 1].mean()

            atr_values = [np.nan] * (period + 1)
            atr_values[period] = first_atr

            for i in range(period + 1, len(df)):
                prev_atr = atr_values[i - 1]
                current_tr = df['true_range'].iloc[i]
                atr_values.append(((period - 1) * prev_atr + current_tr) / period)

            df['atr'] = atr_values
            return df
        except Exception as e:
            logging.error(f"Failed to calculate ATR for {symbol}: {e}")
            return None

    def fetch_daily_bars_incremental(self, symbol: str) -> list:
        """Fetch daily bars incrementally based on existing database data."""

        # Get the last daily bar timestamp from database
        last_timestamp = self.get_last_timestamp(symbol, 'daily_bars')

        if last_timestamp:
            # Fetch from last timestamp to now
            start_date = last_timestamp
            logging.info(f"Incremental sync for {symbol} daily bars: Last date in DB is {last_timestamp}")
        else:
            # No existing data, fetch last 40 days
            start_date = datetime.now() - timedelta(days=40)
            logging.info(f"No existing daily bars for {symbol}, fetching last 40 days")

        end_date = datetime.now()

        # Check if we need to fetch (skip if last bar is from today)
        if last_timestamp and last_timestamp.date() >= datetime.now().date():
            logging.info(f"Daily bars for {symbol} are up to date")
            return []

        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=TimeFrame(1, TimeFrameUnit.Day),
            start=start_date.isoformat() if hasattr(start_date, 'isoformat') else start_date,
            end=end_date.isoformat(),
            feed="iex"
        )

        try:
            bars = self.client.get_stock_bars(request)
            df_new = bars.df.reset_index()

            if df_new.empty:
                logging.warning(f"No new daily bars data received for {symbol}")
                return []

            # Remove duplicates if any
            if last_timestamp:
                df_new = df_new[pd.to_datetime(df_new['timestamp']).dt.date > last_timestamp.date()]

            if df_new.empty:
                logging.info(f"No new daily bars for {symbol}")
                return []

            # Get existing data for ATR calculation
            existing_df = self.get_existing_data(symbol, 'daily_bars')

            if not existing_df.empty:
                # Combine for ATR calculation
                combined_df = pd.concat([existing_df.tail(20), df_new], ignore_index=True)
                combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'], utc=True)
                # if combined_df['timestamp'].dt.tz is None:
                #     combined_df['timestamp'] = combined_df['timestamp'].dt.tz_localize('US/Eastern')
                combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)
                combined_df = self.calculate_atr(symbol, combined_df)

                # Extract only new records
                new_records_start_idx = len(existing_df.tail(20))
                df_new = combined_df.iloc[new_records_start_idx:].copy()
            else:
                df_new = df_new.sort_values('timestamp').reset_index(drop=True)
                df_new = self.calculate_atr(symbol, df_new)

            result = []
            for _, row in df_new.iterrows():
                date_str = pd.to_datetime(row["timestamp"]).date().isoformat()
                result.append({
                    "symbol": row.get("symbol", symbol),
                    "timestamp": date_str,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "vwap": row.get("vwap", None),
                    "atr": row.get("atr", None)
                })

            logging.info(f"Fetched {len(result)} new daily bars for {symbol}")
            return result

        except Exception as e:
            logging.error(f"Failed to fetch daily bars for {symbol}: {e}")
            return []

    def fetch_daily_bars(self, symbol: str, days_back: int = 40) -> list:
        """Fetch daily bars for a symbol (full sync)."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=TimeFrame(1, TimeFrameUnit.Day),
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            feed="iex"
        )

        try:
            bars = self.client.get_stock_bars(request)
            df = bars.df.reset_index()

            if df.empty:
                logging.warning(f"No daily bars data received for {symbol}")
                return []

            df = df.sort_values('timestamp').reset_index(drop=True)
            df = self.calculate_atr(symbol, df)

            result = []
            for _, row in df.iterrows():
                date_str = row["timestamp"].date().isoformat()
                result.append({
                    "symbol": row["symbol"],
                    "timestamp": date_str,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "vwap": row.get("vwap", None),
                    "atr": row["atr"]
                })

            logging.info(f"Fetched {len(result)} daily bars for {symbol}")
            return result
        except Exception as e:
            logging.error(f"Failed to fetch daily bars for {symbol}: {e}")
            return []

    def fetch_1min_session_stats(self, symbol: str, day: str):
        """Fetch 1-min bars for the session and calculate session high, low, and POC."""
        est = self.est_tz
        session_start = est.localize(datetime.strptime(f"{day} 09:30:00", "%Y-%m-%d %H:%M:%S"))
        session_end = est.localize(datetime.strptime(f"{day} 15:59:00", "%Y-%m-%d %H:%M:%S"))

        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=TimeFrame(1, TimeFrameUnit.Minute),
            start=session_start.isoformat(),
            end=session_end.isoformat()
        )

        try:
            bars = self.client.get_stock_bars(request)
            df = bars.df.reset_index()
            if df.empty:
                return None, None, None
            # Get the highest 'high' value from the bars DataFrame
            highest_high = df['high'].max()

            volume_profile_metrics = self.calculate_volume_profile(df, price_levels=70)
            session_high = volume_profile_metrics.value_area_high
            session_low = volume_profile_metrics.value_area_low
            poc = volume_profile_metrics.poc_price

            return highest_high, session_high, session_low, poc
        except Exception as e:
            logging.error(f"Failed to fetch 1-min session stats for {symbol}: {e}")
            return None, None, None

    def calculate_volume_profile(self, df: pd.DataFrame, price_levels: int = 70) -> VolumeProfileMetrics:
        """Calculate volume profile from 1-minute data."""
        if df.empty:
            raise ValueError("DataFrame is empty")

        high_price = df['high'].max()
        low_price = df['low'].min()
        price_range = high_price - low_price

        if price_range == 0:
            raise ValueError("No price movement in the data")

        price_step = price_range / price_levels
        price_bins = np.arange(low_price, high_price + price_step, price_step)

        volume_at_price = {}

        for _, row in df.iterrows():
            bar_low = row['low']
            bar_high = row['high']
            bar_volume = row['volume']

            if bar_volume == 0:
                continue

            affected_levels = []
            for i, price_level in enumerate(price_bins[:-1]):
                level_high = price_bins[i + 1]

                if not (level_high < bar_low or price_level > bar_high):
                    affected_levels.append(price_level)

            if affected_levels:
                volume_per_level = bar_volume / len(affected_levels)
                for level in affected_levels:
                    if level not in volume_at_price:
                        volume_at_price[level] = 0
                    volume_at_price[level] += volume_per_level

        total_volume = sum(volume_at_price.values())
        profile_levels = []

        for price, volume in sorted(volume_at_price.items()):
            percentage = (volume / total_volume) * 100 if total_volume > 0 else 0
            profile_levels.append(VolumeProfileLevel(
                price_level=price,
                volume=int(volume),
                percentage=percentage
            ))

        poc_level = max(profile_levels, key=lambda x: x.volume)
        poc_level.is_poc = True
        poc_price = poc_level.price_level

        value_area_volume = total_volume * 0.72

        sorted_by_price = sorted(profile_levels, key=lambda x: x.price_level)
        poc_index = next(i for i, level in enumerate(sorted_by_price) if level.is_poc)

        current_volume = poc_level.volume
        low_index = high_index = poc_index

        while current_volume < value_area_volume and (low_index > 0 or high_index < len(sorted_by_price) - 1):
            expand_low = low_index > 0
            expand_high = high_index < len(sorted_by_price) - 1

            low_volume = sorted_by_price[low_index - 1].volume if expand_low else 0
            high_volume = sorted_by_price[high_index + 1].volume if expand_high else 0

            if expand_low and (not expand_high or low_volume >= high_volume):
                low_index -= 1
                current_volume += sorted_by_price[low_index].volume
                sorted_by_price[low_index].is_value_area = True
            elif expand_high:
                high_index += 1
                current_volume += sorted_by_price[high_index].volume
                sorted_by_price[high_index].is_value_area = True
            else:
                break

        poc_level.is_value_area = True

        value_area_high = sorted_by_price[high_index].price_level
        value_area_low = sorted_by_price[low_index].price_level

        return VolumeProfileMetrics(
            poc_price=round(poc_price, 2),
            value_area_high=round(value_area_high, 2),
            value_area_low=round(value_area_low, 2),
            total_volume=int(total_volume),
            price_levels=profile_levels
        )

    def store_daily_bars(self, ticker: str, bars: list, incremental: bool = False):
        """Store daily bars in the database with proper incremental handling."""
        if not bars:
            return

        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        inserted_count = 0
        updated_count = 0

        for bar in bars:
            # Check if session stats need to be fetched
            highest_high, session_high, session_low, poc = self.fetch_1min_session_stats(
                bar["symbol"], bar["timestamp"][:10]
            )

            try:
                if incremental:
                    # Try to insert, if it exists update it
                    cursor.execute('''
                        INSERT OR REPLACE INTO daily_bars
                        (symbol, timestamp, open, high, low, close, volume, vwap, 
                         session_high, session_low, poc, atr)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        bar["symbol"], bar["timestamp"], bar["open"], bar["high"],
                        bar["low"], bar["close"], bar["volume"], bar["vwap"],
                        session_high, session_low, poc, bar.get("atr")
                    ))
                    if cursor.rowcount > 0:
                        inserted_count += 1
                else:
                    # Full sync - insert normally
                    cursor.execute('''
                        INSERT INTO daily_bars
                        (symbol, timestamp, open, high, low, close, volume, vwap, 
                         session_high, session_low, poc, atr)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        bar["symbol"], bar["timestamp"], bar["open"], highest_high,
                        bar["low"], bar["close"], bar["volume"], bar["vwap"],
                        session_high, session_low, poc, bar.get("atr")
                    ))
                    inserted_count += 1
            except sqlite3.IntegrityError:
                # Record already exists in incremental mode
                updated_count += 1
            except Exception as e:
                logging.error(f"Error inserting daily bar for {bar['symbol']} on {bar['timestamp']}: {e}")

        conn.commit()
        conn.close()

        if incremental:
            logging.info(f"Incremental: Stored/Updated {inserted_count} daily bars, {updated_count} already existed")
        else:
            logging.info(f"Stored {inserted_count} daily bars in the database")

    def store_data(self, df: pd.DataFrame, table_name: str, incremental: bool = False):
        """Store dataframe in the specified table with incremental support."""
        if df.empty:
            return

        conn = sqlite3.connect(DATABASE_NAME)

        try:
            if incremental:
                # For incremental, use INSERT OR REPLACE
                df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
                logging.info(f"Incrementally stored {len(df)} records in {table_name}")
            else:
                # For full sync, clear existing data first
                symbols = df['symbol'].unique()
                placeholders = ','.join(['?' for _ in symbols])
                conn.execute(f'DELETE FROM {table_name} WHERE symbol IN ({placeholders})', symbols)

                # Insert new data
                df.to_sql(table_name, conn, if_exists='append', index=False)
                logging.info(f"Stored {len(df)} records in {table_name} (full sync)")

        except Exception as e:
            logging.error(f"Error storing data in {table_name}: {str(e)}")
        finally:
            conn.close()

    def collect_all_data(self, incremental: bool = False):
        """Main method to collect data for all tickers and timeframes."""
        logging.info(f"Starting data collection process - Mode: {'INCREMENTAL' if incremental else 'FULL'}")

        # Setup database
        self.setup_database()

        # Load tickers
        tickers = self.load_tickers()
        if not tickers:
            logging.error("No tickers to process")
            return

        # Process each ticker
        for ticker in tickers:
            logging.info(f"Processing {ticker} - Mode: {'INCREMENTAL' if incremental else 'FULL'}")

            if incremental:
                # Incremental sync
                # Fetch and store daily bars incrementally
                daily_bars = self.fetch_daily_bars_incremental(ticker)
                self.store_daily_bars(ticker, daily_bars, incremental=True)

                # Get 5-minute data incrementally
                df_5min = self.get_market_hours_data_incremental(
                    ticker,
                    TimeFrame(5, TimeFrameUnit.Minute),
                    'candles_5min',
                    days_back=10
                )
                if not df_5min.empty:
                    self.store_data(df_5min, 'candles_5min', incremental=True)

                # Get 15-minute data incrementally  
                df_15min = self.get_market_hours_data_incremental(
                    ticker,
                    TimeFrame(15, TimeFrameUnit.Minute),
                    'candles_15min',
                    days_back=10
                )
                if not df_15min.empty:
                    self.store_data(df_15min, 'candles_15min', incremental=True)
            else:
                # Full sync (existing logic)
                daily_bars = self.fetch_daily_bars(ticker, days_back=40)

                # Clear existing data before storing
                conn = sqlite3.connect(DATABASE_NAME)
                cursor = conn.cursor()
                cursor.execute('DELETE FROM daily_bars WHERE symbol = ?', (ticker,))
                conn.commit()
                conn.close()

                self.store_daily_bars(ticker, daily_bars, incremental=False)

                # Get 5-minute data
                df_5min = self.get_market_hours_data(ticker, TimeFrame(5, TimeFrameUnit.Minute))
                if not df_5min.empty:
                    self.store_data(df_5min, 'candles_5min', incremental=False)

                # Get 15-minute data
                df_15min = self.get_market_hours_data(ticker, TimeFrame(15, TimeFrameUnit.Minute))
                if not df_15min.empty:
                    self.store_data(df_15min, 'candles_15min', incremental=False)

        logging.info(f"Data collection completed - Mode: {'INCREMENTAL' if incremental else 'FULL'}")

    def get_data_summary(self):
        """Print summary of stored data with EMA information."""
        conn = sqlite3.connect(DATABASE_NAME)

        # 5-minute data summary
        cursor = conn.execute('''
            SELECT symbol,
                   COUNT(*) as bar_count,
                   MIN(timestamp) as earliest,
                   MAX(timestamp) as latest,
                   AVG(vwap) as avg_vwap,
                   AVG(ema_8) as avg_ema_8,
                   AVG(ema_20) as avg_ema_20,
                   AVG(ema_39) as avg_ema_39
            FROM candles_5min
            WHERE vwap IS NOT NULL
            GROUP BY symbol
        ''')

        print("\n5-Minute Data Summary:")
        print("-" * 120)
        print(
            f"{'Symbol':<10} {'Bars':<6} {'Earliest':<20} {'Latest':<20} {'Avg VWAP':<10} {'Avg EMA8':<10} {'Avg EMA20':<10} {'Avg EMA39':<10}")
        print("-" * 120)

        for row in cursor.fetchall():
            symbol, bar_count, earliest, latest, avg_vwap, avg_ema_8, avg_ema_20, avg_ema_39 = row
            avg_vwap_str = f"{avg_vwap:.2f}" if avg_vwap else "N/A"
            avg_ema_8_str = f"{avg_ema_8:.2f}" if avg_ema_8 else "N/A"
            avg_ema_20_str = f"{avg_ema_20:.2f}" if avg_ema_20 else "N/A"
            avg_ema_39_str = f"{avg_ema_39:.2f}" if avg_ema_39 else "N/A"
            print(
                f"{symbol:<10} {bar_count:<6} {earliest:<20} {latest:<20} {avg_vwap_str:<10} {avg_ema_8_str:<10} {avg_ema_20_str:<10} {avg_ema_39_str:<10}")

        # 15-minute data summary
        cursor = conn.execute('''
            SELECT symbol,
                   COUNT(*) as bar_count,
                   MIN(timestamp) as earliest,
                   MAX(timestamp) as latest,
                   AVG(vwap) as avg_vwap,
                   AVG(ema_8) as avg_ema_8,
                   AVG(ema_20) as avg_ema_20,
                   AVG(ema_39) as avg_ema_39
            FROM candles_15min
            WHERE vwap IS NOT NULL
            GROUP BY symbol
        ''')

        print("\n15-Minute Data Summary:")
        print("-" * 120)
        print(
            f"{'Symbol':<10} {'Bars':<6} {'Earliest':<20} {'Latest':<20} {'Avg VWAP':<10} {'Avg EMA8':<10} {'Avg EMA20':<10} {'Avg EMA39':<10}")
        print("-" * 120)

        for row in cursor.fetchall():
            symbol, bar_count, earliest, latest, avg_vwap, avg_ema_8, avg_ema_20, avg_ema_39 = row
            avg_vwap_str = f"{avg_vwap:.2f}" if avg_vwap else "N/A"
            avg_ema_8_str = f"{avg_ema_8:.2f}" if avg_ema_8 else "N/A"
            avg_ema_20_str = f"{avg_ema_20:.2f}" if avg_ema_20 else "N/A"
            avg_ema_39_str = f"{avg_ema_39:.2f}" if avg_ema_39 else "N/A"
            print(
                f"{symbol:<10} {bar_count:<6} {earliest:<20} {latest:<20} {avg_vwap_str:<10} {avg_ema_8_str:<10} {avg_ema_20_str:<10} {avg_ema_39_str:<10}")

        # Daily bars summary
        cursor = conn.execute('''
            SELECT symbol,
                   COUNT(*) as bar_count,
                   MIN(timestamp) as earliest,
                   MAX(timestamp) as latest,
                   AVG(atr) as avg_atr,
                   AVG(poc) as avg_poc
            FROM daily_bars
            GROUP BY symbol
        ''')

        print("\nDaily Bars Summary:")
        print("-" * 100)
        print(f"{'Symbol':<10} {'Bars':<6} {'Earliest':<20} {'Latest':<20} {'Avg ATR':<10} {'Avg POC':<10}")
        print("-" * 100)

        for row in cursor.fetchall():
            symbol, bar_count, earliest, latest, avg_atr, avg_poc = row
            avg_atr_str = f"{avg_atr:.2f}" if avg_atr else "N/A"
            avg_poc_str = f"{avg_poc:.2f}" if avg_poc else "N/A"
            print(f"{symbol:<10} {bar_count:<6} {earliest:<20} {latest:<20} {avg_atr_str:<10} {avg_poc_str:<10}")

        conn.close()


def main():
    """Main function to run the data collector."""
    import argparse

    # Set up argument parser
    parser = argparse.ArgumentParser(description='Alpaca Data Collector')
    parser.add_argument('--mode', choices=['full', 'incremental'], default='incremental',
                        help='Data collection mode: full or incremental (default: incremental)')
    parser.add_argument('--summary', action='store_true',
                        help='Show data summary after collection')

    args = parser.parse_args()

    # Load API keys
    API_KEY = os.getenv('APCA_API_KEY_ID')
    SECRET_KEY = os.getenv('APCA_API_SECRET_KEY')

    if not API_KEY or not SECRET_KEY:
        print("Error: Please set APCA_API_KEY_ID and APCA_API_SECRET_KEY environment variables")
        return

    try:
        collector = AlpacaDataCollector(API_KEY, SECRET_KEY)

        # Run collection based on mode
        incremental = args.mode == 'incremental'
        collector.collect_all_data(incremental=incremental)

        # Show summary if requested
        if args.summary:
            collector.get_data_summary()

        print(f"\nData collection completed successfully in {args.mode.upper()} mode!")

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()