# watchlist_job_generator.py

from __future__ import annotations

import os
import json
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Tuple

import psycopg2
from psycopg2.extensions import connection as PgConnection


@dataclass
class WatchlistEntry:
    last_trading_day: date
    simulation_date: date
    symbol: str
    close: float
    atr: float
    ema_20: float
    avg_vol10: float


class WatchlistJobGenerator:
    """
    Builds:
      1) Daily watchlist ticker files:
         tickers_backup/test_all_tickers_MMDDYYYY.txt
      2) sim_jobs.json with:
         {
           "jobs": [
             {
               "last_trading_day": "YYYY-MM-DD",
               "simulation_date": "YYYY-MM-DD",
               "ticker_file": "tickers_backup/test_all_tickers_MMDDYYYY.txt"
             }, ...
           ]
         }

    Watchlist for simulation_date S is based on last_trading_day L,
    where L is the previous trading day in daily_bars.
    """

    def __init__(self, dsn: str, output_dir: str = "tickers_backup"):
        """
        dsn: PostgreSQL DSN, e.g.
             'dbname=market_data user=postgres password=xxx host=localhost port=5432'
        output_dir: folder where test_all_tickers_*.txt files are written.
        """
        self._dsn = dsn
        self._output_dir = output_dir

    def _get_connection(self) -> PgConnection:
        return psycopg2.connect(self._dsn)

    # ---------- Public entry point ----------

    def generate_for_range(
        self,
        start_sim_date: date,
        end_sim_date: date,
        jobs_json_path: str = "sim_jobs.json",
    ) -> None:
        """
        Generate:
          - watchlist text files under self._output_dir
          - sim_jobs.json at jobs_json_path

        for simulation dates in [start_sim_date, end_sim_date].
        """
        os.makedirs(self._output_dir, exist_ok=True)

        with self._get_connection() as conn:
            trading_dates = self._get_trading_dates(conn)
            if not trading_dates:
                raise RuntimeError("No trading dates found in daily_bars")

            # Map each trading day -> next trading day
            next_trading = self._build_next_trading_map(trading_dates)

            # Determine which last_trading_days we actually need bars for
            relevant_last_days = self._find_relevant_last_days(
                trading_dates,
                next_trading,
                start_sim_date,
                end_sim_date,
            )

            if not relevant_last_days:
                print("No trading days fall into the requested simulation range.")
                return

            min_last_day = min(relevant_last_days)
            max_last_day = max(relevant_last_days)

            # Fetch candidate symbols that satisfy your rules on those last_trading_days
            candidates_by_date = self._fetch_candidates_by_date(
                conn,
                min_last_day,
                max_last_day,
            )

        # Now build watchlist files + jobs JSON
        jobs_payload = self._write_watchlists_and_build_jobs(
            trading_dates,
            next_trading,
            candidates_by_date,
            start_sim_date,
            end_sim_date,
        )

        with open(jobs_json_path, "w", encoding="utf-8") as f:
            json.dump({"jobs": jobs_payload}, f, indent=2)
        print(f"Written {len(jobs_payload)} jobs to {jobs_json_path}")

    # ---------- Helpers ----------

    def _get_trading_dates(self, conn: PgConnection) -> List[date]:
        """
        Get all distinct trading dates from daily_bars, sorted ascending.
        """
        sql = """
        SELECT DISTINCT timestamp::date AS d
        FROM daily_bars
        ORDER BY d;
        """
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return [r[0] for r in rows]

    def _build_next_trading_map(self, trading_dates: List[date]) -> Dict[date, date]:
        """
        For each trading date d, map to the next trading date d_next.
        The last trading date will not have an entry.
        """
        next_trading: Dict[date, date] = {}
        for i in range(len(trading_dates) - 1):
            current = trading_dates[i]
            nxt = trading_dates[i + 1]
            next_trading[current] = nxt
        return next_trading

    def _find_relevant_last_days(
        self,
        trading_dates: List[date],
        next_trading: Dict[date, date],
        start_sim_date: date,
        end_sim_date: date,
    ) -> List[date]:
        """
        Which last_trading_day dates do we actually need bars for,
        given the simulation date range?
        """
        relevant_last_days: List[date] = []
        for d in trading_dates:
            nxt = next_trading.get(d)
            if not nxt:
                continue
            if start_sim_date <= nxt <= end_sim_date:
                relevant_last_days.append(d)
        return relevant_last_days

    def _fetch_candidates_by_date(
        self,
        conn: PgConnection,
        min_last_day: date,
        max_last_day: date,
    ) -> Dict[date, List[Tuple[str, float, float, float, float]]]:
        """
        Fetch all symbols on dates in [min_last_day, max_last_day] that satisfy:
          atr > 0.8
          close > ema_20
          avg_vol10 > 2,000,000

        Returns:
            { date: [(symbol, close, atr, ema_20, avg_vol10), ...], ... }
        """
        sql = """
        WITH bars AS (
            SELECT
                symbol,
                timestamp::date AS date,
                close,
                atr,
                ema_20,
                volume,
                AVG(volume) OVER (
                    PARTITION BY symbol
                    ORDER BY timestamp::date
                    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                ) AS avg_vol10
            FROM daily_bars
        )
        SELECT
            date,
            symbol,
            close,
            atr,
            ema_20,
            avg_vol10
        FROM bars
        WHERE
            date BETWEEN %s AND %s
            AND atr > 0.9
            AND close > ema_20
            AND avg_vol10 IS NOT NULL
            AND avg_vol10 > 2000000
        ORDER BY date, symbol;
        """

        by_date: Dict[date, List[Tuple[str, float, float, float, float]]] = {}

        with conn.cursor() as cur:
            cur.execute(sql, (min_last_day, max_last_day))
            rows = cur.fetchall()

        for d, symbol, close, atr, ema_20, avg_vol10 in rows:
            by_date.setdefault(d, []).append(
                (symbol, float(close), float(atr), float(ema_20), float(avg_vol10))
            )

        return by_date

    def _write_watchlists_and_build_jobs(
            self,
            trading_dates: List[date],
            next_trading: Dict[date, date],
            candidates_by_date: Dict[date, List[Tuple[str, float, float, float, float]]],
            start_sim_date: date,
            end_sim_date: date,
    ) -> List[Dict[str, str]]:
        """
        For each (last_trading_day -> simulation_date) pair in the window,
        write test_all_tickers_MMDDYYYY.txt and build jobs JSON entries.

        SKIP days where symbols list is empty.
        """
        jobs: List[Dict[str, str]] = []

        for d in trading_dates:
            sim_date = next_trading.get(d)
            if not sim_date:
                continue

            if sim_date < start_sim_date or sim_date > end_sim_date:
                continue

            entries = candidates_by_date.get(d, [])
            symbols = sorted({sym for sym, *_ in entries})

            # SKIP: nothing to write, nothing to add to jobs
            if not symbols:
                continue

            # Build filename based on simulation_date
            filename = f"test_all_tickers_{sim_date.strftime('%m%d%Y')}.txt"
            rel_path = os.path.join(self._output_dir, filename)

            # Write watchlist file
            with open(rel_path, "w", encoding="utf-8") as f:
                f.write("\n".join(symbols) + "\n")

            # Add job entry
            jobs.append(
                {
                    "last_trading_day": d.isoformat(),
                    "simulation_date": sim_date.isoformat(),
                    "ticker_file": f"{self._output_dir}/{filename}",
                }
            )

        return jobs


if __name__ == "__main__":
    # Example usage: generate watchlists for simulation dates
    # from 2025-02-01 to 2025-11-06.
    from datetime import date as _date

    DSN = "dbname=testmarket user=postgres password=admin host=localhost port=5432"

    generator = WatchlistJobGenerator(DSN, output_dir="tickers_backup")
    generator.generate_for_range(
        start_sim_date=_date(2025, 2, 1),
        end_sim_date=_date(2025, 11, 6),
        jobs_json_path="sim_jobs.json",
    )
