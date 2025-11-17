#!/usr/bin/env python3
"""
Alpaca Candlestick Data Collector
Fetches 5min and 15min candlestick data and stores in local SQLite database.
Designed to run after market hours to collect last 100 bars of market hours data.
Enhanced with RSI, Momentum, and VWAP bands for daily bars.
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
DATABASE_NAME = "E:/database/market_data_old.db"
TICKERS_FILE = "special_ticker_list.txt" #"ticker_list.txt"
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

    def setup_database(self):
        """Create database tables if they don't exist."""
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        # Create table for 5min data (without RSI)
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

        # Create table for 15min data (without RSI)
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

        # Add daily bars table with ATR, RSI, Momentum, and VWAP bands columns
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
                    rsi_10 REAL,
                    momentum_10 REAL
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, timestamp)
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

        # Add columns to existing tables if they don't exist
        for column in ['vwap', 'ema_8', 'ema_20', 'ema_39']:
            try:
                cursor.execute(f'ALTER TABLE candles_5min ADD COLUMN {column} REAL')
            except sqlite3.OperationalError:
                pass  # Column already exists

            try:
                cursor.execute(f'ALTER TABLE candles_15min ADD COLUMN {column} REAL')
            except sqlite3.OperationalError:
                pass  # Column already exists

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

    def is_market_hours(self, timestamp: datetime) -> bool:
        """Check if timestamp is within market hours (6:30 AM to 1:00 PM PST)."""
        # Convert to PST if needed
        if timestamp.tzinfo is None:
            # Assume EST if no timezone info
            timestamp = self.est_tz.localize(timestamp)

        pst_time = timestamp.astimezone(self.pst_tz)
        hour_decimal = pst_time.hour + pst_time.minute / 60.0

        return MARKET_START_PST <= hour_decimal <= MARKET_END_PST

    def calculate_rsi(self, df: pd.DataFrame, period: int = 10) -> pd.DataFrame:
        """
        Calculate RSI (Relative Strength Index) for given period.

        Args:
            df: DataFrame with price data
            period: RSI period (default 10)

        Returns:
            DataFrame with RSI column added
        """
        if df.empty or len(df) < period + 1:
            df['rsi_10'] = np.nan
            return df

        df = df.copy()
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Calculate price changes
        df['price_change'] = df['close'].diff()

        # Separate gains and losses
        df['gain'] = df['price_change'].apply(lambda x: x if x > 0 else 0)
        df['loss'] = df['price_change'].apply(lambda x: -x if x < 0 else 0)

        # Calculate initial average gain and loss (SMA for first period)
        avg_gain = df['gain'].iloc[1:period + 1].mean()
        avg_loss = df['loss'].iloc[1:period + 1].mean()

        # Initialize lists for average gains and losses
        avg_gains = [np.nan] * (period + 1)
        avg_losses = [np.nan] * (period + 1)
        avg_gains[period] = avg_gain
        avg_losses[period] = avg_loss

        # Calculate subsequent values using Wilder's smoothing (EMA)
        for i in range(period + 1, len(df)):
            avg_gains.append((avg_gains[i - 1] * (period - 1) + df['gain'].iloc[i]) / period)
            avg_losses.append((avg_losses[i - 1] * (period - 1) + df['loss'].iloc[i]) / period)

        df['avg_gain'] = avg_gains
        df['avg_loss'] = avg_losses

        # Calculate RS and RSI
        df['rs'] = df['avg_gain'] / df['avg_loss'].replace(0, np.nan)
        df['rsi_10'] = 100 - (100 / (1 + df['rs']))

        # Round to 2 decimal places
        df['rsi_10'] = df['rsi_10'].round(2)

        # Clean up temporary columns
        df = df.drop(['price_change', 'gain', 'loss', 'avg_gain', 'avg_loss', 'rs'], axis=1)

        return df

    def calculate_momentum(self, df: pd.DataFrame, period: int = 10) -> pd.DataFrame:
        """
        Calculate Momentum indicator.
        Momentum = (Current Close / Close N periods ago) * 100

        Args:
            df: DataFrame with price data
            period: Momentum period (default 10)

        Returns:
            DataFrame with momentum column added
        """
        if df.empty or len(df) < period + 1:
            df['momentum_10'] = np.nan
            return df

        df = df.copy()
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Calculate momentum as percentage of price N periods ago
        df['momentum_10'] = (df['close'] / df['close'].shift(period)) * 100

        # Round to 2 decimal places
        df['momentum_10'] = df['momentum_10'].round(2)

        return df


    def get_market_hours_data(self, symbol: str, timeframe: TimeFrame, days_back: int = 10) -> pd.DataFrame:
        """Fetch candlestick data and filter for market hours only."""
        end_date = datetime.now(self.est_tz)
        start_date = end_date - timedelta(days=days_back)

        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=timeframe,
            adjustment="all",
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

            # Clean up columns (no RSI for intraday data)
            available_columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume',
                                 'vwap', 'ema_8', 'ema_20', 'ema_39']
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

            # Round to 2 decimal places
            df[f'ema_{period}'] = df[f'ema_{period}'].round(3)

        return df

    def calculate_vwap_series(self, df: pd.DataFrame, include_bands: bool = False,
                              multiplier: float = 1.5) -> pd.DataFrame:
        """
        Calculate VWAP for each bar starting from the first bar of each day.
        Optionally calculate VWAP bands based on standard deviation.

        Args:
            df: DataFrame with OHLCV data
            include_bands: Whether to calculate VWAP upper and lower bands
            multiplier: Standard deviation multiplier for bands (default 1.5)

        Returns:
            DataFrame with VWAP (and optionally bands) columns added
        """
        if df.empty:
            return df

        df = df.copy()
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Convert timestamp to date for grouping by day
        df['date'] = df['timestamp'].dt.date
        vwap_values = []
        vwap_upper_values = []
        vwap_lower_values = []

        # Group by date and calculate VWAP for each day separately
        for date, group in df.groupby('date'):
            # Reset cumulative values for each new day
            cumulative_pv = 0
            cumulative_volume = 0
            typical_prices = []
            volumes = []

            # Sort by timestamp within the day
            group = group.sort_values('timestamp').reset_index(drop=True)

            for index, row in group.iterrows():
                # Calculate typical price for this bar
                typical_price = (row['high'] + row['low'] + row['close']) / 3
                volume = row['volume']

                # Store for band calculation if needed
                typical_prices.append(typical_price)
                volumes.append(volume)

                # Add to cumulative values (reset each day)
                cumulative_pv += typical_price * volume
                cumulative_volume += volume

                # Calculate VWAP up to this point for the current day and round to 2 decimal places
                vwap = round(cumulative_pv / cumulative_volume, 2) if cumulative_volume > 0 else 0.00
                vwap_values.append(vwap)

                # Calculate bands if requested
                if include_bands and cumulative_volume > 0:
                    # Calculate weighted standard deviation up to this point
                    weighted_squared_deviations = 0
                    total_volume_so_far = 0

                    # Calculate the current cumulative VWAP for deviation calculation
                    current_vwap = cumulative_pv / cumulative_volume

                    # Calculate weighted squared deviations for all bars up to this point
                    for i in range(len(typical_prices)):
                        if volumes[i] > 0:
                            deviation = typical_prices[i] - current_vwap
                            weighted_squared_deviations += (deviation ** 2) * volumes[i]
                            total_volume_so_far += volumes[i]

                    # Calculate standard deviation
                    if total_volume_so_far > 0:
                        variance = weighted_squared_deviations / total_volume_so_far
                        std_dev = np.sqrt(variance)

                        # Calculate bands
                        vwap_upper = round(current_vwap + (multiplier * std_dev), 2)
                        vwap_lower = round(current_vwap - (multiplier * std_dev), 2)
                    else:
                        vwap_upper = vwap
                        vwap_lower = vwap

                    vwap_upper_values.append(vwap_upper)
                    vwap_lower_values.append(vwap_lower)

        # Sort the original dataframe and assign VWAP values
        df = df.sort_values('timestamp').reset_index(drop=True)
        df['vwap'] = vwap_values

        # Add bands if calculated
        if include_bands:
            df['vwap_upper'] = vwap_upper_values
            df['vwap_lower'] = vwap_lower_values

        # Remove the helper date column
        df = df.drop('date', axis=1)

        return df

    def calculate_daily_indicators(self, symbol: str, df: pd.DataFrame, period: int = 10):
        """
        Calculate all daily indicators: ATR, RSI, Momentum, and VWAP bands.

        Args:
            symbol: Stock symbol
            df: DataFrame with daily OHLC data
            period: Period for ATR calculation (default 10)

        Returns:
            DataFrame with all indicators calculated
        """
        try:
            if df.empty:
                logging.warning(f"Empty dataframe for {symbol}")
                return df

            # Sort by timestamp
            df = df.sort_values('timestamp').reset_index(drop=True)

            # Calculate ATR
            if len(df) >= period + 1:
                # Calculate True Range for each day
                df['prev_close'] = df['close'].shift(1)
                df['high_low'] = df['high'] - df['low']
                df['high_pc'] = abs(df['high'] - df['prev_close'])
                df['low_pc'] = abs(df['low'] - df['prev_close'])

                # True Range is the maximum of these three values
                df['true_range'] = df[['high_low', 'high_pc', 'low_pc']].max(axis=1)

                # Calculate ATR using Exponential Moving Average (Wilder's method)
                # First ATR is simple average of first 'period' TR values
                first_atr = df['true_range'].iloc[1:period + 1].mean()

                # Initialize ATR values
                atr_values = [np.nan] * (period + 1)
                atr_values[period] = first_atr

                # Calculate subsequent ATR values using Wilder's smoothing
                for i in range(period + 1, len(df)):
                    prev_atr = atr_values[i - 1]
                    current_tr = df['true_range'].iloc[i]
                    # Wilder's smoothing: ATR = ((period - 1) * prev_ATR + current_TR) / period
                    atr_values.append(((period - 1) * prev_atr + current_tr) / period)

                df['atr'] = atr_values

                # Clean up ATR temporary columns
                df = df.drop(['prev_close', 'high_low', 'high_pc', 'low_pc', 'true_range'], axis=1)
            else:
                df['atr'] = np.nan

            # Calculate RSI
            df = self.calculate_rsi(df, period=10)

            # Calculate Momentum
            df = self.calculate_momentum(df, period=10)

            return df

        except Exception as e:
            logging.error(f"Failed to calculate daily indicators for {symbol}: {e}")
            return df

    def fetch_daily_bars(self, symbol: str, days_back: int = 40) -> list:
        """Fetch daily bars for a symbol from Alpaca REST API for the last N days."""
        # Calculate date range
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

            # Sort by timestamp
            df = df.sort_values('timestamp').reset_index(drop=True)

            # Calculate all daily indicators
            df = self.calculate_daily_indicators(symbol, df)

            result = []
            for _, row in df.iterrows():
                date_str = row["timestamp"].date().isoformat()

                # Fetch all session stats including VWAP bands in one call
                highest_high, session_high, session_low, poc, vwap_upper, vwap_lower = self.fetch_1min_session_stats(
                    symbol, date_str
                )

                result.append({
                    "symbol": row["symbol"],
                    "timestamp": date_str,
                    "open": row["open"],
                    "high": highest_high if highest_high else row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "vwap": row.get("vwap", None),
                    "vwap_upper": vwap_upper,
                    "vwap_lower": vwap_lower,
                    "session_high": session_high,
                    "session_low": session_low,
                    "poc": poc,
                    "atr": row.get("atr", None),
                    "rsi_10": row.get("rsi_10", None),
                    "momentum_10": row.get("momentum_10", None)
                })

            logging.info(f"Fetched {len(result)} daily bars for {symbol} with technical indicators and VWAP bands")
            return result
        except Exception as e:
            logging.error(f"Failed to fetch daily bars for {symbol}: {e}")
            return []

    def fetch_1min_session_stats(self, symbol: str, day: str, multiplier: float = 1.5):
        """
        Fetch 1-min bars for the session and calculate session high, low, POC, and VWAP bands.
        All calculations done in a single pass for efficiency.

        Args:
            symbol: Stock symbol
            day: Date string in YYYY-MM-DD format
            multiplier: Standard deviation multiplier for VWAP bands (default 1.5)

        Returns:
            Tuple of (highest_high, session_high, session_low, poc, vwap_upper, vwap_lower)
        """
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
                return None, None, None, None, None, None

            # Get highest high
            highest_high = df['high'].max()

            # Calculate volume profile metrics for session high/low and POC
            volume_profile_metrics = self.calculate_volume_profile(df, price_levels=70)
            session_high = volume_profile_metrics.value_area_high
            session_low = volume_profile_metrics.value_area_low
            poc = volume_profile_metrics.poc_price

            # Calculate VWAP bands using the enhanced method
            df_with_bands = self.calculate_vwap_series(df, include_bands=True, multiplier=multiplier)

            # Get the final (end of day) VWAP band values
            if 'vwap_upper' in df_with_bands.columns and 'vwap_lower' in df_with_bands.columns:
                vwap_upper = df_with_bands['vwap_upper'].iloc[-1]
                vwap_lower = df_with_bands['vwap_lower'].iloc[-1]
            else:
                vwap_upper = None
                vwap_lower = None

            return highest_high, session_high, session_low, poc, vwap_upper, vwap_lower

        except Exception as e:
            logging.error(f"Failed to fetch 1-min session stats for {symbol}: {e}")
            return None, None, None, None, None, None

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

    def store_daily_bars(self, ticker: str, bars: list, incremental: bool = False):
        """Store daily bars in the database, including session stats, VWAP bands, and technical indicators."""
        if not bars:
            return

        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        if incremental == False:
            cursor.execute('DELETE FROM daily_bars WHERE symbol = ?', (ticker,))

        # Insert new daily bars
        inserted_count = 0
        for bar in bars:
            try:
                cursor.execute('''
                               INSERT INTO daily_bars
                               (symbol, timestamp, open, high, low, close, volume, vwap, vwap_upper, vwap_lower,
                                session_high, session_low, poc, atr, rsi_10, momentum_10)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                               ''', (
                                   bar["symbol"], bar["timestamp"], bar["open"], bar["high"], bar["low"],
                                   bar["close"], bar["volume"], bar["vwap"], bar.get("vwap_upper"),
                                   bar.get("vwap_lower"),
                                   bar.get("session_high"), bar.get("session_low"), bar.get("poc"),
                                   bar.get("atr"), bar.get("rsi_10"), bar.get("momentum_10")
                               ))
                inserted_count += 1
            except Exception as e:
                logging.error(f"Error inserting daily bar for {bar['symbol']} on {bar['timestamp']}: {e}")

        conn.commit()
        conn.close()
        logging.info(f"Stored {inserted_count} daily bars in the database with VWAP bands")

    def store_data(self, df: pd.DataFrame, table_name: str):
        """Store dataframe in the specified table."""
        if df.empty:
            return

        conn = sqlite3.connect(DATABASE_NAME)

        try:
            # Clear existing data for these symbols to avoid duplicates
            symbols = df['symbol'].unique()
            placeholders = ','.join(['?' for _ in symbols])
            conn.execute(f'DELETE FROM {table_name} WHERE symbol IN ({placeholders})', symbols)

            # Insert new data
            df.to_sql(table_name, conn, if_exists='append', index=False)
            logging.info(f"Stored {len(df)} records in {table_name}")

        except Exception as e:
            logging.error(f"Error storing data in {table_name}: {str(e)}")
        finally:
            conn.close()

    def collect_all_data(self, incremental: bool = False):
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
            # Fetch and store daily bars with technical indicators and VWAP bands
            daily_bars = self.fetch_daily_bars(ticker, incremental == False and 40 or 1)

            self.store_daily_bars(ticker, daily_bars, incremental)

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
        conn = sqlite3.connect(DATABASE_NAME)

        # 5-minute data summary
        cursor = conn.execute('''
                              SELECT symbol,
                                     COUNT(*)       as bar_count,
                                     MIN(timestamp) as earliest,
                                     MAX(timestamp) as latest,
                                     AVG(vwap)      as avg_vwap,
                                     AVG(ema_8)     as avg_ema_8,
                                     AVG(ema_20)    as avg_ema_20,
                                     AVG(ema_39)    as avg_ema_39
                              FROM candles_5min
                              WHERE vwap IS NOT NULL
                              GROUP BY symbol
                              ''')

        print("\n5-Minute Data Summary:")
        print("-" * 120)
        print(
            f"{'Symbol':<10} {'Bars':<6} {'Earliest':<20} {'Latest':<20} {'Avg VWAP':<10} {'Avg EMA8':<10}  {'Avg EMA20':<10} {'Avg EMA39':<10}")
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
                                     COUNT(*)       as bar_count,
                                     MIN(timestamp) as earliest,
                                     MAX(timestamp) as latest,
                                     AVG(vwap)      as avg_vwap,
                                     AVG(ema_8)     as avg_ema_8,
                                     AVG(ema_20)    as avg_ema_20,
                                     AVG(ema_39)    as avg_ema_39
                              FROM candles_15min
                              WHERE vwap IS NOT NULL
                              GROUP BY symbol
                              ''')

        print("\n15-Minute Data Summary:")
        print("-" * 120)
        print(
            f"{'Symbol':<10} {'Bars':<6} {'Earliest':<20} {'Latest':<20} {'Avg VWAP':<10} {'Avg EMA8':<10}  {'Avg EMA20':<10} {'Avg EMA39':<10}")
        print("-" * 120)

        for row in cursor.fetchall():
            symbol, bar_count, earliest, latest, avg_vwap, avg_ema_8, avg_ema_20, avg_ema_39 = row
            avg_vwap_str = f"{avg_vwap:.2f}" if avg_vwap else "N/A"
            avg_ema_8_str = f"{avg_ema_8:.2f}" if avg_ema_8 else "N/A"
            avg_ema_20_str = f"{avg_ema_20:.2f}" if avg_ema_20 else "N/A"
            avg_ema_39_str = f"{avg_ema_39:.2f}" if avg_ema_39 else "N/A"
            print(
                f"{symbol:<10} {bar_count:<6} {earliest:<20} {latest:<20} {avg_vwap_str:<10} {avg_ema_8_str:<10} {avg_ema_20_str:<10} {avg_ema_39_str:<10}")

        # Daily bars summary with technical indicators and VWAP bands
        cursor = conn.execute('''
                              SELECT symbol,
                                     COUNT(*)         as bar_count,
                                     MIN(timestamp)   as earliest,
                                     MAX(timestamp)   as latest,
                                     AVG(atr)         as avg_atr,
                                     AVG(rsi_10)      as avg_rsi,
                                     AVG(momentum_10) as avg_momentum,
                                     AVG(vwap_upper)  as avg_vwap_upper,
                                     AVG(vwap_lower)  as avg_vwap_lower
                              FROM daily_bars
                              GROUP BY symbol
                              ''')

        print("\nDaily Bars Summary with VWAP Bands:")
        print("-" * 160)
        print(
            f"{'Symbol':<10} {'Bars':<6} {'Earliest':<20} {'Latest':<20} {'Avg ATR':<10} {'Avg RSI':<10} {'Avg Mom':<10} {'Avg Upper':<12} {'Avg Lower':<12}")
        print("-" * 160)

        for row in cursor.fetchall():
            symbol, bar_count, earliest, latest, avg_atr, avg_rsi, avg_momentum, avg_vwap_upper, avg_vwap_lower = row
            avg_atr_str = f"{avg_atr:.2f}" if avg_atr else "N/A"
            avg_rsi_str = f"{avg_rsi:.2f}" if avg_rsi else "N/A"
            avg_mom_str = f"{avg_momentum:.2f}" if avg_momentum else "N/A"
            avg_upper_str = f"{avg_vwap_upper:.2f}" if avg_vwap_upper else "N/A"
            avg_lower_str = f"{avg_vwap_lower:.2f}" if avg_vwap_lower else "N/A"
            print(
                f"{symbol:<10} {bar_count:<6} {earliest:<20} {latest:<20} {avg_atr_str:<10} {avg_rsi_str:<10} {avg_mom_str:<10} {avg_upper_str:<12} {avg_lower_str:<12}")

        conn.close()

    def get_daily_technical_analysis(self, symbol: str, limit: int = 10):
        """Get latest technical indicators for daily bars including VWAP bands."""
        conn = sqlite3.connect(DATABASE_NAME)

        cursor = conn.execute('''
                              SELECT timestamp, close, vwap, vwap_upper, vwap_lower, atr, rsi_10, momentum_10, CASE
                                  WHEN close > vwap_upper THEN 'Above Upper'
                                  WHEN close < vwap_lower THEN 'Below Lower'
                                  ELSE 'Within Bands'
                              END
                              as vwap_position,
                   CASE 
                       WHEN rsi_10 > 70 THEN 'Overbought'
                       WHEN rsi_10 < 30 THEN 'Oversold'
                       ELSE 'Neutral'
                              END
                              as rsi_signal,
                   CASE 
                       WHEN momentum_10 > 100 THEN 'Bullish'
                       WHEN momentum_10 < 100 THEN 'Bearish'
                       ELSE 'Neutral'
                              END
                              as momentum_signal                    
            FROM daily_bars
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT ?
                              ''', (symbol, limit))

        print(f"\nDaily Technical Analysis for {symbol} (Latest {limit} bars):")
        print("-" * 180)
        print(
            f"{'Date':<12} {'Close':<8} {'VWAP':<8} {'Upper':<8} {'Lower':<8} {'Position':<13} {'ATR':<8} {'RSI(10)':<8} {'RSI Sig':<12} {'Mom(10)':<8} {'Mom Sig':<10}")
        print("-" * 180)

        for row in cursor.fetchall():
            timestamp, close, vwap, vwap_upper, vwap_lower, atr, rsi, momentum, vwap_pos, rsi_sig, mom_sig = row
            close_str = f"{close:.2f}" if close else "N/A"
            vwap_str = f"{vwap:.2f}" if vwap else "N/A"
            upper_str = f"{vwap_upper:.2f}" if vwap_upper else "N/A"
            lower_str = f"{vwap_lower:.2f}" if vwap_lower else "N/A"
            atr_str = f"{atr:.2f}" if atr else "N/A"
            rsi_str = f"{rsi:.2f}" if rsi else "N/A"
            mom_str = f"{momentum:.2f}" if momentum else "N/A"
            print(
                f"{timestamp:<12} {close_str:<8} {vwap_str:<8} {upper_str:<8} {lower_str:<8} {vwap_pos:<13} {atr_str:<8} {rsi_str:<8} {rsi_sig:<12} {mom_str:<8} {mom_sig:<10}")

        conn.close()


def main():
    """Main function to run the data collector."""
    # You need to set these environment variables or replace with your actual keys
    API_KEY = os.getenv('APCA_API_KEY_ID')
    SECRET_KEY = os.getenv('APCA_API_SECRET_KEY')
    if not API_KEY or not SECRET_KEY:
        print("Error: Please set APCA_API_KEY_ID and APCA_API_SECRET_KEY environment variables")
        print("Or modify the script to include your keys directly")
        return

    try:
        collector = AlpacaDataCollector(API_KEY, SECRET_KEY)
        collector.collect_all_data()
        collector.get_data_summary()

        # Example: Get technical analysis for the first ticker
        tickers = collector.load_tickers()
        if tickers:
            collector.get_daily_technical_analysis(tickers[0], 10)

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")


if __name__ == "__main__":
    main()