#!/usr/bin/env python3
"""
Alpaca Candlestick Data Collector - PostgreSQL Version
Fetches 5min and 15min candlestick data and stores in PostgreSQL database.
Designed to run after market hours to collect last 100 bars of market hours data.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
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

# PostgreSQL Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'market'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'admin'),
    'port': os.getenv('DB_PORT', '5432')
}

TICKERS_FILE = "ticker_list.txt"
MAX_BARS = 600

# Market hours in PST (6:30 AM to 1:00 PM PST)
MARKET_START_PST = 6.5  # 6:30 AM
MARKET_END_PST = 12.99   # 1:00 PM

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

class AlpacaDataCollector:
    def __init__(self, api_key: str, secret_key: str):
        """Initialize the Alpaca data collector."""
        self.client = StockHistoricalDataClient(api_key, secret_key)
        self.pst_tz = pytz.timezone('US/Pacific')
        self.est_tz = pytz.timezone('US/Eastern')  # Alpaca uses EST

        # Initialize connection pool
        self.connection_pool = SimpleConnectionPool(1, 20, **DB_CONFIG)

    def get_connection(self):
        """Get connection from pool."""
        return self.connection_pool.getconn()

    def return_connection(self, conn):
        """Return connection to pool."""
        self.connection_pool.putconn(conn)

    def setup_database(self):
        """Create database tables if they don't exist."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            # Create table for 5min data
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS candles_5min (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    open DECIMAL(10,2) NOT NULL,
                    high DECIMAL(10,2) NOT NULL,
                    low DECIMAL(10,2) NOT NULL,
                    close DECIMAL(10,2) NOT NULL,
                    volume BIGINT NOT NULL,
                    vwap DECIMAL(10,3),
                    ema_8 DECIMAL(10,3),
                    ema_20 DECIMAL(10,3),
                    ema_39 DECIMAL(10,3),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, timestamp)
                )
            ''')

            # Create table for 15min data
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS candles_15min (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    open DECIMAL(10,2) NOT NULL,
                    high DECIMAL(10,2) NOT NULL,
                    low DECIMAL(10,2) NOT NULL,
                    close DECIMAL(10,2) NOT NULL,
                    volume BIGINT NOT NULL,
                    vwap DECIMAL(10,3),
                    ema_8 DECIMAL(10,3),
                    ema_20 DECIMAL(10,3),
                    ema_39 DECIMAL(10,3),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, timestamp)
                )
            ''')

            # Add daily bars table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_bars (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    timestamp DATE NOT NULL,
                    open DECIMAL(10,2) NOT NULL,
                    high DECIMAL(10,2) NOT NULL,
                    low DECIMAL(10,2) NOT NULL,
                    close DECIMAL(10,2) NOT NULL,
                    volume BIGINT NOT NULL,
                    vwap DECIMAL(10,3),
                    session_high DECIMAL(10,2),
                    session_low DECIMAL(10,2),
                    poc DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, timestamp)
                )
            ''')

            # Add columns if not exist (for upgrades) - PostgreSQL approach
            # Check if columns exist before adding
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='daily_bars' AND column_name IN ('session_high', 'session_low', 'poc')
            """)
            existing_cols = [row[0] for row in cursor.fetchall()]

            for column in ['session_high', 'session_low', 'poc']:
                if column not in existing_cols:
                    cursor.execute(f'ALTER TABLE daily_bars ADD COLUMN {column} DECIMAL(10,2)')

            # Check and add columns for candlestick tables
            for table in ['candles_5min', 'candles_15min']:
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='{table}' AND column_name IN ('vwap', 'ema_8', 'ema_20', 'ema_39')
                """)
                existing_cols = [row[0] for row in cursor.fetchall()]

                for column in ['vwap', 'ema_8', 'ema_20', 'ema_39']:
                    if column not in existing_cols:
                        cursor.execute(f'ALTER TABLE {table} ADD COLUMN {column} DECIMAL(10,3)')

            # Create indexes for better query performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_5min_symbol_timestamp ON candles_5min(symbol, timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_15min_symbol_timestamp ON candles_15min(symbol, timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_symbol_timestamp ON daily_bars(symbol, timestamp)')

            conn.commit()
            logging.info("Database setup completed")

        except Exception as e:
            logging.error(f"Database setup error: {str(e)}")
            conn.rollback()
        finally:
            cursor.close()
            self.return_connection(conn)

    def load_tickers(self) -> List[str]:
        """Load tickers from the text file."""
        if not os.path.exists(TICKERS_FILE):
            logging.error(f"Tickers file {TICKERS_FILE} not found")
            return []

        with open(TICKERS_FILE, 'r') as f:
            tickers = [line.strip().upper() for line in f if line.strip()]

        logging.info(f"Loaded {len(tickers)} tickers: {tickers}")
        return tickers

    def is_market_hours(self, timestamp: datetime) -> bool:
        """Check if timestamp is within market hours (6:30 AM to 1:00 PM PST)."""
        # Convert to PST if needed
        if timestamp.tzinfo is None:
            # Assume EST if no timezone info
            timestamp = self.est_tz.localize(timestamp)

        pst_time = timestamp.astimezone(self.pst_tz)
        hour_decimal = pst_time.hour + pst_time.minute / 60.0

        return MARKET_START_PST <= hour_decimal <= MARKET_END_PST

    def get_market_hours_data(self, symbol: str, timeframe: TimeFrame, days_back: int = 10) -> pd.DataFrame:
        """Fetch candlestick data and filter for market hours only."""
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
            available_columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'vwap', 'ema_8', 'ema_20', 'ema_39']
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
            # Calculate EMA using pandas built-in function
            df[f'ema_{period}'] = df['close'].ewm(span=period, adjust=False).mean()

            # Round to 3 decimal places
            df[f'ema_{period}'] = df[f'ema_{period}'].round(3)

        return df

    def calculate_vwap_series(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate VWAP for each bar starting from the first bar of each day."""
        if df.empty:
            return df

        df = df.copy()
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Convert timestamp to date for grouping by day
        df['date'] = df['timestamp'].dt.date
        vwap_values = []

        # Group by date and calculate VWAP for each day separately
        for date, group in df.groupby('date'):
            # Reset cumulative values for each new day
            cumulative_pv = 0
            cumulative_volume = 0

            # Sort by timestamp within the day
            group = group.sort_values('timestamp').reset_index(drop=True)

            for index, row in group.iterrows():
                # Calculate typical price for this bar
                typical_price = (row['high'] + row['low'] + row['close']) / 3
                volume = row['volume']

                # Add to cumulative values (reset each day)
                cumulative_pv += typical_price * volume
                cumulative_volume += volume

                # Calculate VWAP up to this point for the current day and round to 2 decimal places
                vwap = round(cumulative_pv / cumulative_volume, 2) if cumulative_volume > 0 else 0.00
                vwap_values.append(vwap)

        # Sort the original dataframe and assign VWAP values
        df = df.sort_values('timestamp').reset_index(drop=True)
        df['vwap'] = vwap_values

        # Remove the helper date column
        df = df.drop('date', axis=1)

        return df

    def fetch_daily_bars(self, symbol: str, day: str) -> list:
        """Fetch daily bars for a symbol from Alpaca REST API."""
        # Parse the day string to datetime
        start_date = datetime.strptime(day, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)

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
            result = []
            for _, row in df.iterrows():
                result.append({
                    "symbol": row["symbol"],
                    "timestamp": row["timestamp"].date().isoformat(),
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "vwap": row.get("vwap", None)
                })
            return result
        except Exception as e:
            logging.error(f"Failed to fetch daily bars for {symbol}: {e}")
            return []

    def fetch_1min_session_stats(self, symbol: str, day: str):
        """Fetch 1-min bars for the session and calculate session high, low, and POC."""
        # Session times in EST
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
            volume_profile_metrics = self.calculate_volume_profile(df, price_levels=70)
            # Session high/low
            session_high = volume_profile_metrics.value_area_high
            session_low = volume_profile_metrics.value_area_low

            poc = volume_profile_metrics.poc_price

            return session_high, session_low, poc
        except Exception as e:
            logging.error(f"Failed to fetch 1-min session stats for {symbol}: {e}")
            return None, None, None

    def calculate_volume_profile(self, df: pd.DataFrame, price_levels: int = 70) -> VolumeProfileMetrics:
        """
        Calculate volume profile from 1-minute data

        Args:
            df: DataFrame with OHLCV data
            price_levels: Number of price levels to create

        Returns:
            VolumeProfileMetrics object
        """
        if df.empty:
            raise ValueError("DataFrame is empty")

        # Get price range
        high_price = df['high'].max()
        low_price = df['low'].min()
        price_range = high_price - low_price

        if price_range == 0:
            raise ValueError("No price movement in the data")

        # Create price levels
        price_step = price_range / price_levels
        price_bins = np.arange(low_price, high_price + price_step, price_step)

        # Calculate volume at each price level
        volume_at_price = {}

        for _, row in df.iterrows():
            # Distribute volume across the OHLC range for each minute
            bar_low = row['low']
            bar_high = row['high']
            bar_volume = row['volume']

            if bar_volume == 0:
                continue

            # Find which price levels this bar touches
            affected_levels = []
            for i, price_level in enumerate(price_bins[:-1]):
                level_high = price_bins[i + 1]

                # Check if this price level overlaps with the bar's range
                if not (level_high < bar_low or price_level > bar_high):
                    affected_levels.append(price_level)

            # Distribute volume evenly across affected levels
            if affected_levels:
                volume_per_level = bar_volume / len(affected_levels)
                for level in affected_levels:
                    if level not in volume_at_price:
                        volume_at_price[level] = 0
                    volume_at_price[level] += volume_per_level

        # Convert to sorted list
        total_volume = sum(volume_at_price.values())
        profile_levels = []

        for price, volume in sorted(volume_at_price.items()):
            percentage = (volume / total_volume) * 100 if total_volume > 0 else 0
            profile_levels.append(VolumeProfileLevel(
                price_level=price,
                volume=int(volume),
                percentage=percentage
            ))

        # Find Point of Control (POC) - price level with highest volume
        poc_level = max(profile_levels, key=lambda x: x.volume)
        poc_level.is_poc = True
        poc_price = poc_level.price_level

        # Calculate Value Area (70% of total volume)
        value_area_volume = total_volume * 0.72

        # Find value area by expanding from POC
        sorted_by_price = sorted(profile_levels, key=lambda x: x.price_level)
        poc_index = next(i for i, level in enumerate(sorted_by_price) if level.is_poc)

        # Expand from POC to include 70% of volume
        current_volume = poc_level.volume
        low_index = high_index = poc_index

        while current_volume < value_area_volume and (low_index > 0 or high_index < len(sorted_by_price) - 1):
            # Check which direction to expand (higher volume gets priority)
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

        # Mark POC as part of value area
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

    def store_daily_bars(self, bars: list):
        """Store daily bars in the database, including session stats."""
        if not bars:
            return

        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            for bar in bars:
                # Fetch session stats for this day/symbol
                session_high, session_low, poc = self.fetch_1min_session_stats(bar["symbol"], bar["timestamp"][:10])
                try:
                    cursor.execute('''
                        INSERT INTO daily_bars
                        (symbol, timestamp, open, high, low, close, volume, vwap, session_high, session_low, poc)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, timestamp) DO NOTHING
                    ''', (
                        bar["symbol"], bar["timestamp"], bar["open"], bar["high"], bar["low"],
                        bar["close"], bar["volume"], bar["vwap"], session_high, session_low, poc
                    ))
                except Exception as e:
                    logging.error(f"Error inserting daily bar: {e}")
            conn.commit()
        except Exception as e:
            logging.error(f"Error in store_daily_bars: {str(e)}")
            conn.rollback()
        finally:
            cursor.close()
            self.return_connection(conn)

    def store_data(self, df: pd.DataFrame, table_name: str):
        """Store dataframe in the specified table."""
        if df.empty:
            return

        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            # Clear existing data for these symbols to avoid duplicates
            symbols = df['symbol'].unique()
            placeholders = ','.join(['%s' for _ in symbols])
            cursor.execute(f'DELETE FROM {table_name} WHERE symbol IN ({placeholders})', symbols)

            # Insert new data
            columns = df.columns.tolist()
            values = df.values.tolist()

            placeholders = ','.join(['%s' for _ in columns])
            insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"

            cursor.executemany(insert_query, values)
            conn.commit()
            logging.info(f"Stored {len(df)} records in {table_name}")

        except Exception as e:
            logging.error(f"Error storing data in {table_name}: {str(e)}")
            conn.rollback()
        finally:
            cursor.close()
            self.return_connection(conn)

    def collect_all_data(self):
        """Main method to collect data for all tickers and timeframes."""
        logging.info("Starting data collection process")

        # Setup database
        self.setup_database()

        # Load tickers
        tickers = self.load_tickers()
        if not tickers:
            logging.error("No tickers to process")
            return

        # Process each ticker
        for ticker in tickers:
            logging.info(f"Processing {ticker}")
            # Fetch and store daily bars
            today_str = datetime.now().strftime("%Y-%m-%d")
            daily_bars = self.fetch_daily_bars(ticker, today_str)
            self.store_daily_bars(daily_bars)

            # Get 5-minute data
            df_5min = self.get_market_hours_data(ticker, TimeFrame(5, TimeFrameUnit.Minute))
            if not df_5min.empty:
                self.store_data(df_5min, 'candles_5min')

            # Get 15-minute data
            df_15min = self.get_market_hours_data(ticker, TimeFrame(15, TimeFrameUnit.Minute))
            if not df_15min.empty:
                self.store_data(df_15min, 'candles_15min')

        logging.info("Data collection completed")

    def get_data_summary(self):
        """Print summary of stored data with EMA information."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            # 5-minute data summary
            cursor.execute('''
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
            print(f"{'Symbol':<10} {'Bars':<6} {'Earliest':<20} {'Latest':<20} {'Avg VWAP':<10} {'Avg EMA8':<10}  {'Avg EMA20':<10} {'Avg EMA39':<10}")
            print("-" * 120)

            for row in cursor.fetchall():
                symbol, bar_count, earliest, latest, avg_vwap, avg_ema_8, avg_ema_20, avg_ema_39 = row
                avg_vwap_str = f"{float(avg_vwap):.2f}" if avg_vwap else "N/A"
                avg_ema_8_str = f"{float(avg_ema_8):.2f}" if avg_ema_8 else "N/A"
                avg_ema_20_str = f"{float(avg_ema_20):.2f}" if avg_ema_20 else "N/A"
                avg_ema_39_str = f"{float(avg_ema_39):.2f}" if avg_ema_39 else "N/A"
                print(f"{symbol:<10} {bar_count:<6} {earliest:<20} {latest:<20} {avg_vwap_str:<10} {avg_ema_8_str:<10} {avg_ema_20_str:<10} {avg_ema_39_str:<10}")

        except Exception as e:
            logging.error(f"Error in get_data_summary: {str(e)}")
        finally:
            cursor.close()
            self.return_connection(conn)

    def get_ema_analysis(self, symbol: str, table_name: str = 'candles_5min', limit: int = 10):
        """Get latest EMA values and crossover signals for a symbol."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            cursor.execute(f'''
                SELECT timestamp, close, vwap, ema_20, ema_39,
                       CASE WHEN ema_20 > ema_39 THEN 'Bullish' ELSE 'Bearish' END as signal
                FROM {table_name}
                WHERE symbol = %s AND ema_20 IS NOT NULL AND ema_39 IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT %s
            ''', (symbol, limit))

            print(f"\nEMA Analysis for {symbol} (Latest {limit} bars from {table_name}):")
            print("-" * 100)
            print(f"{'Timestamp':<20} {'Close':<8} {'VWAP':<8} {'EMA20':<8} {'EMA39':<8} {'Signal':<8}")
            print("-" * 100)

            for row in cursor.fetchall():
                timestamp, close, vwap, ema_20, ema_39, signal = row
                close_str = f"{float(close):.2f}" if close else "N/A"
                vwap_str = f"{float(vwap):.2f}" if vwap else "N/A"
                ema_20_str = f"{float(ema_20):.2f}" if ema_20 else "N/A"
                ema_39_str = f"{float(ema_39):.2f}" if ema_39 else "N/A"
                print(f"{timestamp:<20} {close_str:<8} {vwap_str:<8} {ema_20_str:<8} {ema_39_str:<8} {signal:<8}")

        except Exception as e:
            logging.error(f"Error in get_ema_analysis: {str(e)}")
        finally:
            cursor.close()
            self.return_connection(conn)

    def __del__(self):
        """Close connection pool on object destruction."""
        if hasattr(self, 'connection_pool') and self.connection_pool:
            self.connection_pool.closeall()

def main():
    """Main function to run the data collector."""
    # You need to set these environment variables or replace with your actual keys
    API_KEY = os.getenv('APCA_API_KEY_ID')
    SECRET_KEY = os.getenv('APCA_API_SECRET_KEY')

    if not API_KEY or not SECRET_KEY:
        print("Error: Please set APCA_API_KEY_ID and APCA_API_SECRET_KEY environment variables")
        return

    try:
        collector = AlpacaDataCollector(API_KEY, SECRET_KEY)
        collector.collect_all_data()
        collector.get_data_summary()

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")

if __name__ == "__main__":
    main()