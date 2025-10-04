import aiohttp
import sqlite3
import pytz
from datetime import datetime, timedelta, time
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()
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

@dataclass
class ZigZagSignal:
    timestamp: datetime
    signal_type: str
    price: float
    uptrend: bool
    barHigh: float
    barLow: float
    barVwap: float
    barOpen: float
    barClose: float
    ema20: Optional[float] = None
    ema39: Optional[float] = None
    resistance: Optional[float] = None
    support: Optional[float] = None
@dataclass
class Config:
    API_KEY = os.environ.get("APCA_API_KEY_ID")
    API_SECRET = os.environ.get("APCA_API_SECRET_KEY")
    BASE_URL: str = "https://data.alpaca.markets/v2"
    DATABASE_NAME: str = "e:/database/market_data.db"

class AlpacaService:
    def __init__(self):
        self.headers = {
            "APCA-API-KEY-ID": Config.API_KEY,
            "APCA-API-SECRET-KEY": Config.API_SECRET
        }
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
            conn = sqlite3.connect(Config.DATABASE_NAME)
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

    async def get_realtime_bars(self, symbol: str, timeframe: str, last_ema20: float, last_ema39: float,
                                since: Optional[datetime] = None) -> List[Dict]:
        """Fetch real-time bars from Alpaca API and calculate VWAP and incremental EMAs."""
        url = f"{Config.BASE_URL}/stocks/bars"
        utc = pytz.UTC
        today = datetime.now(self.pst_tz).date()
        dt_pst = self.pst_tz.localize(datetime.combine(today, time(6, 30)))
        dt_utc = dt_pst.astimezone(utc)

        params = {
            'symbols': symbol,
            'timeframe': timeframe,
            'start': dt_utc.isoformat(),
            'end': datetime.now(utc).isoformat(),
            'adjustment': 'raw',
            'sort': 'asc'  # Changed to ascending for chronological order
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        bars = []
                        current_time_utc = datetime.now(utc)

                        if symbol in data.get('bars', {}):
                            raw_bars = []

                            # First pass: collect and filter bars
                            for bar in data['bars'][symbol]:
                                utc_time = datetime.fromisoformat(bar['t'].replace('Z', '+00:00'))

                                # Skip bars that are too recent
                                time_diff_seconds = (current_time_utc - utc_time).total_seconds()
                                if time_diff_seconds < int(timeframe[:-3]) * 60 - 0.1:
                                    continue

                                raw_bars.append({
                                    'timestamp': utc_time,
                                    'open': float(bar['o']),
                                    'high': float(bar['h']),
                                    'low': float(bar['l']),
                                    'close': float(bar['c']),
                                    'volume': int(bar['v'])
                                })

                            # Sort by timestamp to ensure chronological order
                            raw_bars.sort(key=lambda x: x['timestamp'])

                            # Second pass: calculate VWAP and incremental EMAs
                            cumulative_pv = 0
                            cumulative_volume = 0
                            current_ema20 = last_ema20 if last_ema20 else None
                            current_ema39 = last_ema39 if last_ema39 else None

                            # EMA multipliers
                            alpha_20 = 2 / (20 + 1)  # 0.095238
                            alpha_39 = 2 / (39 + 1)  # 0.05

                            for bar in raw_bars:
                                # Calculate typical price for VWAP
                                typical_price = (bar['high'] + bar['low'] + bar['close']) / 3

                                # Update cumulative values for VWAP
                                cumulative_pv += typical_price * bar['volume']
                                cumulative_volume += bar['volume']

                                # Calculate VWAP
                                vwap = round(cumulative_pv / cumulative_volume, 2) if cumulative_volume > 0 else 0.00

                                # Calculate incremental EMAs
                                close_price = bar['close']

                                if current_ema20 is not None:
                                    current_ema20 = round((close_price * alpha_20) + (current_ema20 * (1 - alpha_20)),
                                                          2)
                                else:
                                    current_ema20 = close_price  # First value

                                if current_ema39 is not None:
                                    current_ema39 = round((close_price * alpha_39) + (current_ema39 * (1 - alpha_39)),
                                                          2)
                                else:
                                    current_ema39 = close_price  # First value

                                # Add calculated values to the bar
                                bar['vwap'] = vwap
                                bar['ema20'] = current_ema20
                                bar['ema39'] = current_ema39

                                bars.append(bar)

                        return bars
                    else:
                        print(f"Error fetching bars for {symbol}: {response.status}")
                        return []
        except Exception as e:
            print(f"Exception fetching bars for {symbol}: {e}")
            return []
    
    def is_bar_in_market_hours(self, timestamp: datetime) -> bool:
        """Check if a bar timestamp is within market hours."""
        # Convert to PST if needed
        if timestamp.tzinfo is None:
            timestamp = self.est_tz.localize(timestamp)
        
        pst_time = timestamp.astimezone(self.pst_tz)
        bar_time = pst_time.time()
        
        # Check weekday
        if pst_time.weekday() >= 5:  # Weekend
            return False
            
        return self.market_start_pst <= bar_time <= self.market_end_pst

    async def get_combined_bars(self, symbol: str, timeframe: str = "15min", limit: int = 100) -> List[Dict]:
        """
        Get combined bars from database and real-time data.
        During market hours: database + real-time data
        After hours: database data only
        """
        # Always get database data first
        db_bars = self.get_database_bars(symbol, timeframe, limit)

        # If not in market hours, just return database data
        if not self.is_market_hours():
            return db_bars[-limit:] if len(db_bars) > limit else db_bars

        # During market hours, combine with real-time data
        # Get the timestamp and EMAs of the latest database bar
        latest_db_timestamp = None
        latest_bar_ema20 = None
        latest_bar_ema39 = None

        if db_bars:
            latest_db_timestamp = db_bars[-1]['timestamp']
            latest_bar_ema20 = db_bars[-1].get('ema20')
            latest_bar_ema39 = db_bars[-1].get('ema39')

        # Fetch real-time data with EMA continuation
        rt_timeframe = timeframe.replace("min", "Min")  # Convert to API format
        realtime_bars = await self.get_realtime_bars(
            symbol,
            rt_timeframe,
            latest_bar_ema20 or 0.0,
            latest_bar_ema39 or 0.0,
            latest_db_timestamp
        )

        # Combine database and real-time data
        combined_bars = db_bars.copy()

        # Add real-time bars that are newer than database data
        for rt_bar in realtime_bars:
            if not latest_db_timestamp or rt_bar['timestamp'] > latest_db_timestamp:
                # Check for duplicates based on timestamp
                if not any(bar['timestamp'] == rt_bar['timestamp'] for bar in combined_bars):
                    combined_bars.append(rt_bar)

        # Sort by timestamp to ensure chronological order
        combined_bars.sort(key=lambda x: x['timestamp'])

        # Limit to requested number of bars (keep the most recent ones)
        if len(combined_bars) > limit:
            combined_bars = combined_bars[-limit:]

        return combined_bars
    
    async def get_5min_bars(self, symbol: str, limit: int = 100) -> List[Dict]:
        """Get combined 5-minute bars."""
        return await self.get_combined_bars(symbol, "5min", limit)
    
    async def get_15min_bars(self, symbol: str, limit: int = 100) -> List[Dict]:
        """Get combined 15-minute bars."""
        return await self.get_combined_bars(symbol, "15min", limit)
    
    async def get_last_quote_async(self, session: aiohttp.ClientSession, symbol: str):
        """Get the latest quote for a symbol."""
        url = f"{Config.BASE_URL}/stocks/{symbol}/quotes/latest"
        try:
            async with session.get(url, headers=self.headers) as response:
                response.raise_for_status()
                data = await response.json()
                return data["quote"]
        except Exception as e:
            print(f"Error fetching quote for {symbol}: {e}")
            return None
    
    def get_data_info(self, symbol: str) -> Dict:
        """Get information about available data for a symbol."""
        info = {"symbol": symbol}
        
        try:
            # Check database data
            for timeframe in ["5min", "15min"]:
                db_bars = self.get_database_bars(symbol, timeframe, 1000)  # Get all available
                if db_bars:
                    info[f"db_{timeframe}"] = {
                        "count": len(db_bars),
                        "earliest": db_bars[0]['timestamp'],
                        "latest": db_bars[-1]['timestamp']
                    }
                else:
                    info[f"db_{timeframe}"] = {"count": 0}
            
            # Market status
            info["market_open"] = self.is_market_hours()
            info["current_time_pst"] = datetime.now(self.pst_tz)
            
        except Exception as e:
            print(f"Error getting data info for {symbol}: {e}")
        
        return info

class ZigZagIndicator:
    def __init__(self, use_close_to_confirm: bool = True):
        self.use_close = use_close_to_confirm
        self.states: Dict[str, ZigZagState] = {}
        self.signals: Dict[str, List[ZigZagSignal]] = {}

    import pandas as pd

    def calculate(self, symbol: str, df: pd.DataFrame) -> List[ZigZagSignal]:
        """
        Calculate ZigZag signals from a DataFrame.
        The DataFrame must have columns: 'timestamp', 'open', 'high', 'low', 'close', 'vwap', 'ema20', 'ema39'.
        """
        if df.empty or len(df) < 2:
            return []

        bars = df.to_dict('records')
        return self.calculate_zigzag(symbol, bars)

    def calculate_zigzag(self, symbol: str, bars: List[Dict]) -> List[ZigZagSignal]:
        if not bars or len(bars) < 2:
            return []

        if symbol not in self.states:
            self.states[symbol] = ZigZagState()

        if symbol not in self.signals:
            self.signals[symbol] = []

        state = self.states[symbol]
        signals = []

        for i, bar in enumerate(bars):
            high = bar['high']
            low = bar['low']
            close = bar['close']
            timestamp = bar['timestamp']
            vwap = bar['vwap']

            if state.uptrend is None:
                prev_close = bars[i - 1]['close'] if i > 0 else close
                state.uptrend = close > prev_close
                continue

            state.newtop = False
            state.newbot = False
            state.changed = False

            if state.uptrend:
                confirmation_condition = (
                    close < state.toplow if self.use_close
                    else low < state.toplow
                ) if state.toplow is not None else False

                if state.tophigh is None or high > state.tophigh:
                    state.tophigh = high
                    state.toplow = low
                    state.newtop = True

                # confirmation_condition = (
                #     close < state.toplow if self.use_close
                #     else low < state.toplow
                # ) if state.toplow is not None else False

                if confirmation_condition:
                    state.bothigh = high
                    state.botlow = low
                    state.newbot = True
                    state.changed = True
                    state.uptrend = False

                    signal = ZigZagSignal(
                        timestamp=timestamp,
                        signal_type='top_confirmation',
                        price=high,
                        uptrend=False,
                        resistance=state.tophigh,
                        barHigh=high,
                        barLow=low,
                        barVwap=vwap,
                        barOpen=bar['open'],
                        barClose=bar['close'],
                        ema20=bar.get('ema20'),
                        ema39=bar.get('ema39')


                    )
                    signals.append(signal)

            else:
                confirmation_condition = (
                    close > state.bothigh if self.use_close
                    else high > state.bothigh
                ) if state.bothigh is not None else False

                if state.botlow is None or low < state.botlow:
                    state.bothigh = high
                    state.botlow = low
                    state.newbot = True

                # confirmation_condition = (
                #     close > state.bothigh if self.use_close
                #     else high > state.bothigh
                # ) if state.bothigh is not None else False

                if confirmation_condition:
                    state.tophigh = high
                    state.toplow = low
                    state.changed = True
                    state.newtop = True
                    state.uptrend = True

                    signal = ZigZagSignal(
                        timestamp=timestamp,
                        signal_type='bottom_confirmation',
                        price=low,
                        uptrend=True,
                        support=state.botlow,
                        barHigh=high,
                        barLow=low,
                        barVwap=vwap,
                        barOpen=bar['open'],
                        barClose=bar['close'],
                        ema20=bar.get('ema20'),
                        ema39=bar.get('ema39')
                    )
                    signals.append(signal)

        return signals


def range_check(low: float, mid: float, high: float, threshold: float) -> bool:
    """
    Returns True if both:
      - Percentage above 'mid'
      - Percentage below 'low'
    are greater than the input threshold.

    Otherwise returns False.
    """
    if not (low <= mid <= high):
       return False  # invalid input

    total_range = high - low
    if total_range == 0:
        return False  # no range means no distribution

    above_mid = (high - mid) / total_range * 100
    below_mid = (mid - low) / total_range * 100

    return (above_mid > threshold) and (below_mid > threshold)


# Example usage:
# print(range_check(30.23, 30.68, 30.84, 20))  # should be False since below_low = 0


# Example usage function
async def example_usage():
    """Example of how to use the enhanced service."""
    with open("test_ticker_list.txt", 'r') as f:
        tickers = [line.strip().upper() for line in f if line.strip()]

    service = AlpacaService()
    for ticker in tickers:
        symbol = ticker
        # Get combined 15-minute bars
        # bars =  await service.get_15min_bars(symbol, limit=50)
        bars = service.get_database_bars(symbol, limit=50)
        # print(f"Retrieved {len(bars)} 15-minute bars for {symbol}")
        # for bar in bars:
        #     print(f"{symbol} - {bar['timestamp']} : O:{bar['open']} H:{bar['high']} L:{bar['low']} C:{bar['close']} V:{bar['volume']} VWAP:{bar['vwap']} EMA20:{bar.get('ema20')} EMA39:{bar.get('ema39')}")
        if not bars:
            continue
        zigzag = ZigZagIndicator(use_close_to_confirm=True)
        signals = zigzag.calculate_zigzag(symbol, bars)
        pst_tz = pytz.timezone('US/Pacific')
        for signal in signals:
            signal.timestamp = signal.timestamp.astimezone(pst_tz)

        # print(f"Generated {len(signals)} ZigZag signals for {symbol}")
        for signal in signals:
            print(f"{symbol} - {signal.timestamp} : {signal.signal_type} at {signal.price} (Uptrend: {signal.uptrend})")
            check = range_check(signal.barLow, signal.barVwap, signal.barHigh, threshold=10)
            if check :
                print(f"----------{signal.timestamp} - {signal.signal_type} at {signal.price} (Uptrend: {signal.uptrend})")

        if not signals:
            continue

        # Get the most recent bar timestamp for comparison
        latest_bar = max(bars, key=lambda bar: bar['timestamp'])
        latest_bar_time = latest_bar['timestamp']

        # Filter signals from the latest 5-minute candle (within 5 minutes of latest bar)
        latest_signals = [
            s for s in signals
            if abs((s.timestamp - latest_bar_time).total_seconds()) <= 300  # 5 minutes = 300 seconds
        ]

        for signal in latest_signals:
            if signal.signal_type == 'bottom_confirmation' :
                print(f"{signal.timestamp.astimezone(pst_tz)} - {symbol} Bottom at {signal.price} (Uptrend: {signal.uptrend})")


    
    # # Get combined 5-minute bars
    # bars_5min = await service.get_5min_bars("AAPL", limit=50)
    # print(f"Retrieved {len(bars_5min)} 5-minute bars for AAPL")
    #
    # # Get data info
    # info = service.get_data_info("AAPL")
    # print(f"Data info: {info}")
    #
    # # Get latest quote (if needed)
    # async with aiohttp.ClientSession() as session:
    #     quote = await service.get_last_quote_async(session, "AAPL")
    #     print(f"Latest quote: {quote}")

if __name__ == "__main__":
    # p = abs(57.299-57.288)
    # print(p)
    import asyncio
    asyncio.run(example_usage())
