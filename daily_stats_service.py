from dataclasses import dataclass
from typing import Optional, Dict
import sqlite3

DATABASE_NAME = "E:/database/market_data.db"

@dataclass
class DailyStats:
    symbol: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float]
    session_high: Optional[float]
    session_low: Optional[float]
    poc: Optional[float]

class DailyStatsService:
    def __init__(self, db_path: str = DATABASE_NAME):
        self.db_path = db_path
        self._cache: Dict[str, Dict[str, DailyStats]] = {}

    def _load_stats_for_date(self, date: str):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT symbol, timestamp, open, high, low, close, volume, vwap, session_high, session_low, poc
            FROM daily_bars
            WHERE timestamp = ?
        ''', (date,))
        rows = cursor.fetchall()
        conn.close()
        stats = {}
        for row in rows:
            stats[row[0]] = DailyStats(
                symbol=row[0],
                date=row[1],
                open=row[2],
                high=row[3],
                low=row[4],
                close=row[5],
                volume=row[6],
                vwap=row[7],
                session_high=row[8],
                session_low=row[9],
                poc=row[10]
            )
        self._cache[date] = stats

    def get_day_stats(self, date: str, symbol: str) -> Optional[DailyStats]:
        if date not in self._cache:
            self._load_stats_for_date(date)
        return self._cache[date].get(symbol)