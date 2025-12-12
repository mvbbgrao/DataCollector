import csv
import re
from pathlib import Path

LOG_PATH = Path("strategy.log")          # input log file
OUT_STATS_CSV = Path("trade_stats.csv")  # job-level stats
OUT_TRADES_CSV = Path("trades.csv")     # trade-level details


def clean_money(value: str) -> str:
    """
    Strip $, commas and whitespace from money fields.
    Returns the raw string; you can cast to float if you want.
    """
    if value is None:
        return ""
    return value.replace("$", "").replace(",", "").strip()


def parse_log(path: Path):
    text = path.read_text(encoding="utf-8", errors="ignore")
    lines = text.splitlines()

    stats_rows = []
    trade_rows = []

    current_job = None  # header info for the current job

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # ------------------------------------------------------------------
        # 1) Detect job header
        # ------------------------------------------------------------------
        if stripped.startswith("RUNNING SIMULATION JOB #"):
            # Example: RUNNING SIMULATION JOB #43
            m = re.search(r"RUNNING SIMULATION JOB #(\d+)", stripped)
            if m:
                job_number = int(m.group(1))

                # Look ahead for header lines
                last_trading_day = None
                simulation_date = None
                ticker_file = None

                for j in range(i + 1, min(i + 10, len(lines))):
                    ln = lines[j].strip()
                    if ln.startswith("last_trading_day:"):
                        last_trading_day = ln.split(":", 1)[1].strip()
                    elif ln.startswith("simulation_date"):
                        simulation_date = ln.split(":", 1)[1].strip()
                    elif ln.startswith("ticker_file"):
                        ticker_file = ln.split(":", 1)[1].strip()

                current_job = {
                    "job_number": job_number,
                    "last_trading_day": last_trading_day,
                    "simulation_date": simulation_date,
                    "ticker_file": ticker_file,
                }

        # ------------------------------------------------------------------
        # 2) Detect TRADE SUMMARY and parse individual trades
        # ------------------------------------------------------------------
        if stripped == "=== TRADE SUMMARY ===" and current_job is not None:
            # Skip lines until after the header row & dashed separator
            j = i + 1
            # Skip blank lines and header lines
            while j < len(lines):
                hdr = lines[j].strip()
                if hdr.startswith("ID") and "Symbol" in hdr:
                    # Next line should be dashes
                    j += 1
                    if j < len(lines) and set(lines[j].strip()) == {"-"}:
                        j += 1
                    break
                j += 1

            # Now parse trade rows until blank line or stats block
            while j < len(lines):
                ln = lines[j].rstrip("\n")
                s = ln.strip()

                # End of trade summary block
                if s == "" or s.startswith("--- TRADE STATISTICS ---"):
                    break

                # Skip any separator lines just in case
                if s.startswith("ID ") or set(s) == {"-"}:
                    j += 1
                    continue

                parts = s.split()
                # Expect at least 10 tokens:
                # ID, Symbol, EntryTime, EntryPrice, Stop, ExitTime, ExitPrice,
                # P&L, Size, Reason (possibly more pieces for Reason -> join)
                if len(parts) >= 10:
                    trade_id = parts[0]
                    symbol = parts[1]
                    entry_time = parts[2]
                    entry_price = parts[3]
                    stop = parts[4]
                    exit_time = parts[5]
                    exit_price = parts[6]
                    pnl_raw = parts[7]
                    size = parts[8]
                    reason = " ".join(parts[9:])

                    trade_rows.append({
                        "job_number": current_job.get("job_number"),
                        "last_trading_day": current_job.get("last_trading_day"),
                        "simulation_date": current_job.get("simulation_date"),
                        "ticker_file": current_job.get("ticker_file"),

                        "trade_id": trade_id,
                        "symbol": symbol,
                        "entry_time": entry_time,
                        "entry_price": entry_price,
                        "stop": stop,
                        "exit_time": exit_time,
                        "exit_price": exit_price,
                        "pnl": clean_money(pnl_raw),
                        "size": size,
                        "reason": reason,
                    })

                j += 1

        # ------------------------------------------------------------------
        # 3) Detect TRADE STATISTICS block
        # ------------------------------------------------------------------
        if stripped == "--- TRADE STATISTICS ---" and current_job is not None:
            stats = {}
            j = i + 1
            while j < len(lines):
                ln = lines[j].rstrip("\n")
                s = ln.strip()
                if s == "" or s.startswith("====") or s.startswith("RUNNING SIMULATION JOB #"):
                    break
                if ":" in ln:
                    key, val = ln.split(":", 1)
                    key = key.strip()
                    val = val.strip()
                    stats[key] = val
                j += 1

            stats_rows.append({
                "job_number": current_job.get("job_number"),
                "last_trading_day": current_job.get("last_trading_day"),
                "simulation_date": current_job.get("simulation_date"),
                "ticker_file": current_job.get("ticker_file"),
                "Number of trades": stats.get("Number of trades"),
                "Winning trades": stats.get("Winning trades"),
                "Losing trades": stats.get("Losing trades"),
                "Win rate": stats.get("Win rate"),
                "Total P&L": stats.get("Total P&L"),
                "Average win": stats.get("Average win"),
                "Average loss": stats.get("Average loss"),
                "Profit factor": stats.get("Profit factor"),
                "Largest win": stats.get("Largest win"),
                "Largest loss": stats.get("Largest loss"),
                "Max drawdown": stats.get("Max drawdown"),
                "Sharpe ratio": stats.get("Sharpe ratio"),
            })

        i += 1

    return stats_rows, trade_rows


def write_stats_csv(rows, out_path: Path):
    if not rows:
        print("No TRADE STATISTICS blocks found.")
        return

    fieldnames = [
        "job_number",
        "last_trading_day",
        "simulation_date",
        "ticker_file",
        "Number of trades",
        "Winning trades",
        "Losing trades",
        "Win rate",
        "Total P&L",
        "Average win",
        "Average loss",
        "Profit factor",
        "Largest win",
        "Largest loss",
        "Max drawdown",
        "Sharpe ratio",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"Wrote {len(rows)} stats rows to {out_path}")


def write_trades_csv(rows, out_path: Path):
    if not rows:
        print("No trades found.")
        return

    fieldnames = [
        "job_number",
        "last_trading_day",
        "simulation_date",
        "ticker_file",
        "trade_id",
        "symbol",
        "entry_time",
        "entry_price",
        "stop",
        "exit_time",
        "exit_price",
        "pnl",
        "size",
        "reason",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"Wrote {len(rows)} trades to {out_path}")

if __name__ == "__main__":
    # rows = parse_log(
    #     Path('C:/source/TSV2/simulation_results/sim_large_dataset_2025-02-04_20251208-233454_V8_4_until_may.txt'))
    # write_csv(rows, Path('C:/Users/mvbbg/Documents/Sim_Results/v8_4/V8_4_until_may.csv'))
    # rows = parse_log(Path('C:/source/TSV2_Dup/simulation_results/sim_large_dataset_2025-02-03_20251209-074434_baseline_alm.txt'))
    # stats_rows, trade_rows = parse_log(Path('C:/source/TSV2_Dup/simulation_results/sim_large_dataset_2025-02-03_20251209-162157_baseline.txt'))
    # write_stats_csv(stats_rows, Path(Path('C:/Users/mvbbg/Documents/Sim_Results/BASELINE/baseline_stats.csv')))
    # write_trades_csv(trade_rows, Path(Path('C:/Users/mvbbg/Documents/Sim_Results/BASELINE/baseline_trades.csv')))
    # rows = parse_log(
    #     Path('c:/source/TSV_Dup_V61/simulation_results/sim_large_dataset_2025-02-03_20251208-233505_v8_4_sept_nov.txt'))
    # write_csv(rows, Path('C:/Users/mvbbg/Documents/Sim_Results/v8_4/V8_4_sept_nov.csv'))
    stats_rows, trade_rows = parse_log(
        Path('C:/source/TSV2_Dup/simulation_results/sim_large_dataset_2025-02-03_20251211-085748_8_5_baseline.txt'))
    write_stats_csv(stats_rows, Path(Path('C:/Users/mvbbg/Documents/Sim_Results/v9/v_8_5_baseline_stats.csv')))
    write_trades_csv(trade_rows, Path(Path('C:/Users/mvbbg/Documents/Sim_Results/v9/v_8_5_baseline_trades.csv')))