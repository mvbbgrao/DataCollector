from dataclasses import dataclass
from datetime import datetime,time
from typing import Optional
from enum import Enum
import pytz
import sqlite3
from typing import List, Dict

@dataclass
class ZigZagState:
    uptrend: Optional[bool] = None
    tophigh: Optional[float] = None
    toplow: Optional[float] = None
    bothigh: Optional[float] = None
    botlow: Optional[float] = None
    newtop: bool = False
    newbot: bool = False
    changed: bool = False
class ZigZagSignalType(Enum):
    BOTTOM = "BOTTOM"
    TOP = "TOP"
@dataclass
class ZigZagSignal:
    timestamp: datetime
    signal_type: ZigZagSignalType
    price: float
    resistance: Optional[float] = None
    support: Optional[float] = None

@dataclass
class Candle:
    """Represents a single candlestick"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float] = None  # Volume Weighted Average Price
    ema8: Optional[float] = None
    ema20: Optional[float] = None
    ema39: Optional[float] = None

    @property
    def is_upper_wick_long(self) -> bool:
        return self.high - max(self.open, self.close) > self.range_size * 0.3

    @property
    def is_bull_bar(self) -> bool:
        return self.close > self.open and self.body_size > self.range_size * 0.5

    @property
    def is_strong_bull_bar(self) -> bool:
        return self.close > self.open and self.body_size > self.range_size * 0.7

    @property
    def is_green(self) -> bool:
        return self.close > self.open

    @property
    def is_red(self) -> bool:
        return self.close < self.open

    @property
    def body_size(self) -> float:
        return abs(self.close - self.open)

    @property
    def range_size(self) -> float:
        return self.high - self.low

    def is_inside_bar(self, previous_candle: 'Candle') -> bool:
        """Check if this candle is an inside bar relative to previous candle"""
        return (self.high <= previous_candle.high and
                self.low >= previous_candle.low)


class CandleStickBarService:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._cache = {}  # { (symbol, timeframe): [bars] }
        self.pst_tz = pytz.timezone('US/Pacific')
        self.est_tz = pytz.timezone('US/Eastern')

        # Market hours in PST (6:30 AM to 1:00 PM PST)
        self.market_start_pst = time(6, 30)
        self.market_end_pst = time(13, 0)

    def is_market_hours(self) -> bool:
        """Check if current time is within market hours (6:30 AM to 1:00 PM PST)."""
        now_pst = datetime.now(self.pst_tz).time()

        # Check if it's a weekday (Monday = 0, Sunday = 6)
        if datetime.now(self.pst_tz).weekday() >= 5:  # Saturday or Sunday
            return False

        return self.market_start_pst <= now_pst <= self.market_end_pst

    def get_database_bars(self, symbol: str, timeframe: str = "15min", limit: int = 100) -> List[Dict]:
        """Fetch bars from local database with EMA values."""
        table_name = f"candles_{timeframe}"

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            query = f"""
                SELECT timestamp, open, high, low, close, volume, vwap, ema_20, ema_39
                FROM {table_name}
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """
            # AND timestamp < '2025-08-28 14:14:00+00:00'
            cursor.execute(query, (symbol.upper(), limit))
            rows = cursor.fetchall()

            bars = []
            for row in rows:
                bars.append({
                    'timestamp': datetime.fromisoformat(row[0]),
                    'open': float(row[1]),
                    'high': float(row[2]),
                    'low': float(row[3]),
                    'close': float(row[4]),
                    'volume': int(row[5]),
                    'vwap': float(row[6]) if row[6] is not None else 0.0,
                    'ema20': float(row[7]) if row[7] is not None else None,
                    'ema39': float(row[8]) if row[8] is not None else None,
                })

            # Reverse to get chronological order (oldest first)
            bars.reverse()
            conn.close()

            return bars

        except sqlite3.Error as e:
            print(f"Database error fetching {symbol} bars: {e}")
            return []
        except Exception as e:
            print(f"Error fetching database bars for {symbol}: {e}")
            return []

    def _load_bars_from_db(self, symbol: str, timeframe: str = "15min", limit: int = 100):
        bars = self.get_database_bars(symbol, timeframe, limit)
        self._cache[(symbol.upper(), timeframe)] = bars
        return bars

    def get_bars(self, symbol: str, timeframe: str = "15min", limit: int = 100):
        key = (symbol.upper(), timeframe)
        if key not in self._cache or len(self._cache[key]) < limit:
            return self._load_bars_from_db(symbol, timeframe, limit)
        # Return only up to the requested limit
        return self._cache[key][-limit:]