from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import sqlite3

class TradeDirection(Enum):
    LONG = "LONG"
    SHORT = "SHORT"

class ExitReason(Enum):
    STOP_LOSS = "STOP_LOSS"
    TARGET = "TARGET"
    TRAILING_STOP = "TRAILING_STOP"
    TECHNICAL_EXIT = "TECHNICAL_EXIT"
    TIME_EXIT = "TIME_EXIT"
    MANUAL_EXIT = "MANUAL_EXIT"
@dataclass
class TradePosition:
    symbol: str
    entry_price: float
    quantity: int
    direction: TradeDirection
    entry_time: datetime
    stop_loss: Optional[float] = None
    target: Optional[float] = None
    is_active: bool = True
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    exit_reason: Optional[ExitReason] = None
    pnl: float = 0.0



class TradePositionRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._setup_table()

    def _setup_table(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trade_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                entry_price REAL,
                quantity INTEGER,
                direction TEXT,
                entry_time TEXT,
                stop_loss REAL,
                target REAL,
                is_active INTEGER,
                exit_price REAL,
                exit_time TEXT,
                exit_reason TEXT,
                pnl REAL
            )
        ''')
        conn.commit()
        conn.close()

    def add_position(self, position: TradePosition):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO trade_positions (
                symbol, entry_price, quantity, direction, entry_time,
                stop_loss, target, is_active, exit_price, exit_time, exit_reason, pnl
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            position.symbol,
            position.entry_price,
            position.quantity,
            position.direction.value,
            position.entry_time.isoformat(),
            position.stop_loss,
            position.target,
            int(position.is_active),
            position.exit_price,
            position.exit_time.isoformat() if position.exit_time else None,
            position.exit_reason.value if position.exit_reason else None,
            position.pnl
        ))
        conn.commit()
        conn.close()

    def update_position(self, position_id: int, position: TradePosition):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE trade_positions SET
                symbol = ?, entry_price = ?, quantity = ?, direction = ?, entry_time = ?,
                stop_loss = ?, target = ?, is_active = ?, exit_price = ?, exit_time = ?, exit_reason = ?, pnl = ?
            WHERE id = ?
        ''', (
            position.symbol,
            position.entry_price,
            position.quantity,
            position.direction.value,
            position.entry_time.isoformat(),
            position.stop_loss,
            position.target,
            int(position.is_active),
            position.exit_price,
            position.exit_time.isoformat() if position.exit_time else None,
            position.exit_reason.value if position.exit_reason else None,
            position.pnl,
            position_id
        ))
        conn.commit()
        conn.close()

    def get_position_by_id(self, position_id: int) -> Optional[TradePosition]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM trade_positions WHERE id = ?', (position_id,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return TradePosition(
                symbol=row[1],
                entry_price=row[2],
                quantity=row[3],
                direction=TradeDirection(row[4]),
                entry_time=datetime.fromisoformat(row[5]),
                stop_loss=row[6],
                target=row[7],
                is_active=bool(row[8]),
                exit_price=row[9],
                exit_time=datetime.fromisoformat(row[10]) if row[10] else None,
                exit_reason=ExitReason(row[11]) if row[11] else None,
                pnl=row[12]
            )
        return None

    def get_all_positions(self) -> List[TradePosition]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM trade_positions')
        rows = cursor.fetchall()
        conn.close()
        positions = []
        for row in rows:
            positions.append(TradePosition(
                symbol=row[1],
                entry_price=row[2],
                quantity=row[3],
                direction=TradeDirection(row[4]),
                entry_time=datetime.fromisoformat(row[5]),
                stop_loss=row[6],
                target=row[7],
                is_active=bool(row[8]),
                exit_price=row[9],
                exit_time=datetime.fromisoformat(row[10]) if row[10] else None,
                exit_reason=ExitReason(row[11]) if row[11] else None,
                pnl=row[12]
            ))
        return positions