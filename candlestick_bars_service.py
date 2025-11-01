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

    band_width: Optional[float] = None
    band_width_pct: Optional[float] = None
    width_to_atr_ratio: Optional[float] = None
    touches_upper: bool = False
    touches_lower: bool = False
    price_vs_vwap: Optional[float] = None
    # ========= Derived 1-bar geometry (cheap, per-candle) =========
    # These let you avoid re-deriving basics for every detector.
    range: Optional[float] = None  # high - low
    body: Optional[float] = None  # abs(close - open)
    body_frac: Optional[float] = None  # body / max(range, eps)
    upper_wick: Optional[float] = None  # high - max(open, close)
    lower_wick: Optional[float] = None  # min(open, close) - low
    buying_pressure: Optional[float] = None  # (close - low)/range clipped to [0,1]
    momentum_score: Optional[float] = 0
    # ========= Order-flow proxies (OHLCV-only) =========
    # CLV in [-1,1]; delta_proxy = CLV * volume (Chaikin-style)
    clv: Optional[float] = None
    delta_proxy: Optional[float] = None
    vwap_delta: Optional[float] = None  # vwap - prev_vwap (requires prev vwap)
    composite_delta: Optional[float] = None  # weighted combo of delta_proxy & vwap_delta
    delta_confidence: Optional[int] = None  # 0–100 confidence in delta_proxy direction
    # Rolling/cached:
    cum_delta: Optional[
        float] = None  # cumulative sum of delta_proxy over your chosen lookback (or since session start)

    # ========= Volume context (for “spikes” & absorption) =========
    vol_mean: Optional[float] = None  # rolling mean volume (lookback you choose)
    vol_std: Optional[float] = None  # rolling std volume
    vol_z: Optional[float] = None  # (volume - vol_mean) / max(vol_std, eps)

    # ========= Volatility / regime context =========
    # Helpful for “small progress vs ATR/range” checks.
    range_vs_atr: Optional[float] = None  # range / atr (if atr available)
    atr_percent_of_price: Optional[float] = None  # atr / close

    # ========= Pattern detectors (cached results) =========
    # Microstructure (2-5 bar)
    micro_pattern: Optional[str] = None  # e.g., 'inside_bar_bullish_breakout', 'two_bar_bearish_reversal', 'none'
    micro_confidence: Optional[int] = None  # 0–100

    # Absorption (1–K bar eval with volume context)
    absorption_type: Optional[str] = None  # 'buying_absorption'|'selling_absorption'|'neutral_absorption'|'none'
    absorption_strength: Optional[int] = None  # 0–100

    # Divergence (price vs delta proxy over window)
    divergence_type: Optional[str] = None  # 'bullish_divergence'|'bearish_divergence'|'none'
    divergence_strength: Optional[int] = None  # 0–100

    # ========= Summary decision (nice for downstream consumers) =========
    # A single candle can store the *current* decision/outlook computed at that bar.
    of_pattern: Optional[str] = None  # “Order Flow Positive Delta”, etc.
    of_signal: Optional[str] = None  # “Bullish bias maintained”, etc.
    direction: Optional[str] = None  # 'bullish'|'bearish'|'neutral'
    strength_label: Optional[str] = None  # 'Strong'|'Moderate'|'Weak'
    strength_score: Optional[int] = None  # 0–100 (numeric variant if you prefer scoring)

    # (Optional) housekeeping fields to know how these were computed
    lookback_used: Optional[int] = None  # e.g., divergence/volume lookback actually used
    notes: Optional[str] = None  # free-form debug/explainability text

    session_id: Optional[str] = None  # e.g., '2025-09-15' in exchange TZ
    in_rth: Optional[bool] = None  # True if in Regular Trading Hours
    cum_delta_session: Optional[float] = None  # cumulative delta for *this* session only
    vol_mean_session: Optional[float] = None
    vol_std_session: Optional[float] = None
    vol_z_session: Optional[float] = None
    session_index: Optional[int] = None  # 0-based index within today's session

    @property
    def is_upper_wick_long(self) -> bool:
        return self.high - max(self.open, self.close) > self.range_size * 0.3

        # @property
        # def is_bull_bar(self) -> bool:
        #     return self.close > self.open and self.body_size > self.range_size * 0.5

    @property
    def is_z_bull_bar(self) -> bool:
        """
        Determine if a candle is a strong bull bar.

        Args:
            avg_volume (float, optional): Average volume (to compare relative strength).
            body_threshold (float): Minimum ratio of body size to total range (default 0.6 = 60%).
            close_threshold (float): Minimum ratio of close location within range (default 0.7 = 70%).

        Returns:
            bool: True if candle qualifies as a strong bull bar, else False.
        """
        avg_volume = None
        body_threshold = 0.5
        close_threshold = 0.5
        # Avoid division by zero
        if self.high == self.low:
            return False

        # Candle characteristics
        body = self.close - self.open
        total_range = self.high - self.low

        # If candle closed lower than opened, not bullish
        if body <= 0:
            return False

        body_ratio = body / total_range
        close_position = (self.close - self.low) / total_range  # 0 = low, 1 = high

        # Volume check (if provided)
        volume_ok = True
        if self.volume is not None and avg_volume is not None:
            volume_ok = self.volume >= avg_volume

        return (body_ratio >= body_threshold) and (close_position >= close_threshold) and volume_ok

    @property
    def is_bull_bar(self) -> bool:
        """
        Check if a candle is a NEUTRAL bull bar.

        Neutral bull bar:
          - Close > Open (bullish)
          - Not strong (body% < strong threshold or close% < strong threshold)
          - Not weak   (body% >= weak threshold AND close% >= weak threshold)

        Args:
            body_strong (float): Strong body% threshold (default 0.6).
            close_strong (float): Strong close% threshold (default 0.7).
            body_weak (float): Weak body% threshold (default 0.4).
            close_weak (float): Weak close% threshold (default 0.5).

        Returns:
            bool: True if neutral bull bar, else False.
        """

        body_strong = 0.6
        close_strong = 0.7
        body_weak = 0.4
        close_weak = 0.5

        if self.high == self.low:  # avoid division by zero
            return False

        body = self.close - self.open
        rng = self.high - self.low
        body_ratio = body / rng
        close_pos = (self.close - self.low) / rng  # 0 = low, 1 = high

        if body <= 0:  # not bullish
            return False

        # Must be bullish, but not too strong or weak
        # is_not_strong = (body_ratio < body_strong or close_pos < close_strong)
        # is_not_weak = (body_ratio >= body_weak and close_pos >= close_weak)

        is_weak = (body_ratio < body_weak and close_pos < close_weak)

        return is_weak == False and not self.is_upper_wick_long

    # @property
    # def is_strong_bull_bar(self) -> bool:
    #     return self.close > self.open and self.body_size > self.range_size * 0.7
    @property
    def is_strong_bull_bar(self) -> bool:
        """
        Determine if a candle is a strong bull bar.

        Args:
            avg_volume (float, optional): Average volume (to compare relative strength).
            body_threshold (float): Minimum ratio of body size to total range (default 0.6 = 60%).
            close_threshold (float): Minimum ratio of close location within range (default 0.7 = 70%).

        Returns:
            bool: True if candle qualifies as a strong bull bar, else False.
        """
        avg_volume = None
        body_threshold = 0.6
        close_threshold = 0.7
        # Avoid division by zero
        if self.high == self.low:
            return False

        # Candle characteristics
        body = self.close - self.open
        total_range = self.high - self.low

        # If candle closed lower than opened, not bullish
        if body <= 0:
            return False

        body_ratio = body / total_range
        close_position = (self.close - self.low) / total_range  # 0 = low, 1 = high

        # Volume check (if provided)
        volume_ok = True
        if self.volume is not None and avg_volume is not None:
            volume_ok = self.volume >= avg_volume

        return (body_ratio >= body_threshold) and (close_position >= close_threshold) and volume_ok

    @property
    def is_super_strong_bull_bar(self) -> bool:
        """
        Determine if a candle is a strong bull bar.

        Args:
            avg_volume (float, optional): Average volume (to compare relative strength).
            body_threshold (float): Minimum ratio of body size to total range (default 0.6 = 60%).
            close_threshold (float): Minimum ratio of close location within range (default 0.7 = 70%).

        Returns:
            bool: True if candle qualifies as a strong bull bar, else False.
        """
        avg_volume = None
        body_threshold = 0.85
        close_threshold = 0.85
        # Avoid division by zero
        if self.high == self.low:
            return False

        # Candle characteristics
        body = self.close - self.open
        total_range = self.high - self.low

        # If candle closed lower than opened, not bullish
        if body <= 0:
            return False

        body_ratio = body / total_range
        close_position = (self.close - self.low) / total_range  # 0 = low, 1 = high

        # Volume check (if provided)
        volume_ok = True
        if self.volume is not None and avg_volume is not None:
            volume_ok = self.volume >= avg_volume

        return (body_ratio >= body_threshold) and (close_position >= close_threshold) and volume_ok

    @property
    def is_bear_bar(self) -> bool:

        return self.close < self.open and (self.body_size > self.range_size * 0.5 or (
                    self.body_size > self.range_size * 0.35 and self.is_upper_wick_long))

    @property
    def is_strong_bear_bar(self) -> bool:
        return self.close < self.open and self.body_size > self.range_size * 0.7

    @property
    def is_weak_bear_bar(self):
        """
        Determines if a candle qualifies as a weak bear bar based on structural characteristics.

        Parameters:
        -----------
        open_price : float
            Opening price of the candle
        high : float
            High price of the candle
        low : float
            Low price of the candle
        close : float
            Closing price of the candle

        Returns:
        --------
        dict : Contains 'is_weak_bear': bool, 'score': float (0-100), 'reasons': list
        """

        # Basic validation
        if self.close >= self.open:
            return False

        # Calculate structural metrics
        total_range = self.high - self.low
        body_size = self.open - self.close  # Positive for bear bar
        upper_wick = self.high - self.open
        lower_wick = self.close - self.low

        # Avoid division by zero
        if total_range == 0:
            return False

        # Calculate ratios
        body_to_range_ratio = body_size / total_range
        lower_wick_to_body_ratio = lower_wick / body_size if body_size > 0 else float('inf')
        close_position = (self.close - self.low) / total_range  # 0 = at low, 1 = at high

        weakness_score = 0
        reasons = []

        # 1. Small body relative to range (weak characteristic)
        if body_to_range_ratio < 0.3:
            weakness_score += 25
            reasons.append(f'Small body ({body_to_range_ratio:.1%} of range)')
        elif body_to_range_ratio < 0.5:
            weakness_score += 15
            reasons.append(f'Moderate body size ({body_to_range_ratio:.1%} of range)')

        # 2. Long lower wick (rejection of lower prices)
        if lower_wick_to_body_ratio >= 2.0:
            weakness_score += 30
            reasons.append(f'Long lower wick ({lower_wick_to_body_ratio:.1f}x body size)')
        elif lower_wick_to_body_ratio >= 1.0:
            weakness_score += 20
            reasons.append(f'Significant lower wick ({lower_wick_to_body_ratio:.1f}x body size)')

        # 3. Closing position (recovery from lows)
        if close_position >= 0.7:
            weakness_score += 25
            reasons.append(f'Closes near highs ({close_position:.1%} of range)')
        elif close_position >= 0.5:
            weakness_score += 15
            reasons.append(f'Closes in upper half ({close_position:.1%} of range)')

        # 6. Doji-like characteristics
        if body_to_range_ratio < 0.1 and lower_wick > upper_wick:
            weakness_score += 15
            reasons.append('Doji-like with lower wick bias')

        # Final assessment
        is_weak = weakness_score >= 50  # Threshold for weak bear bar

        return is_weak

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

    @property
    def length_in_percentage_to_open(self) -> float:
        return ((self.high - self.low) / self.open) * 100 if self.open != 0 else 0.0

    def is_inside_bar(self, previous_candle: 'Candle') -> bool:
        """Check if this candle is an inside bar relative to previous candle"""
        return (self.high <= previous_candle.high and
                self.low >= previous_candle.low)

    def is_bearish_engulfing(self, previous_candle: 'Candle') -> bool:
        """Check if this candle forms a bearish engulfing pattern with the previous candle"""
        return (self.is_red and previous_candle.is_green and
                self.open > previous_candle.close and
                self.close < previous_candle.open)

    def is_insider_bar_with_overlapping_body(self, previous_candle: 'Candle', tick_size: float = 0.01) -> bool:
        """Check if this candle is an inside bar with overlapping body relative to previous candle"""
        tolerance = tick_size if self.is_upper_wick_long else 0.0

        return (self.is_inside_bar(previous_candle) and
                not (self.open > previous_candle.close + tolerance or
                     self.close < previous_candle.open))

    def is_sitting_on_support(self, support_level: float) -> bool:
        tolerance = 0.01
        on_support = (
                         ((self.low - tolerance <= support_level) & (
                                     self.low + self.range_size * 0.25 >= support_level) & (
                                      self.close > support_level + 2 * tolerance))
                     ) and self.is_bull_bar
        return on_support


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

    def get_database_bars(self, symbol: str, timeframe: str = "15min", limit: int = 26) -> List[Dict]:
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

    def _load_bars_from_db(self, symbol: str, timeframe: str = "15min", limit: int = 26):
        bars = self.get_database_bars(symbol, timeframe, limit)
        # Convert bars (list of dicts) to list of Candle objects
        candle_list = [Candle(**bar) for bar in bars]

        self._cache[(symbol.upper(), timeframe)] = candle_list
        return candle_list

    def get_bars(self, symbol: str, timeframe: str = "15min", limit: int = 26):
        key = (symbol.upper(), timeframe)
        if key not in self._cache or len(self._cache[key]) < limit:
            return self._load_bars_from_db(symbol, timeframe, limit)
        # Return only up to the requested limit
        return self._cache[key][-limit:]


if __name__ == "__main__":
    service = CandleStickBarService(db_path="e:/database/market_data.db")
    bars = service.get_bars("RGTI", "15min", 26)
    for bar in bars:
        print(f"{bar.timestamp.astimezone(service.pst_tz)} - Is Bull Bar: {bar.is_bull_bar} - Is Strong Bull Bar: {bar.is_strong_bull_bar} - Is Super Strong Bull Bar: {bar.is_super_strong_bull_bar}")

