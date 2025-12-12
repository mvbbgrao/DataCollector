#!/usr/bin/env python3
"""
Parse trading strategy V8 log files and convert to CSV format.
Usage: python log_to_csv.py <input_log_file> [output_csv_file]
"""

import re
import csv
import sys
from pathlib import Path


def parse_log_file(log_path: str) -> list[dict]:
    """Parse the trading strategy log file and extract entries."""

    entries = []
    current_entry = None

    # Header pattern: SYMBOL - YYYY-MM-DD HH:MM:SS
    header_pattern = re.compile(
        r'INFO trading\.strategy: (\w+) - (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})$'
    )

    # Separator pattern
    separator_pattern = re.compile(r'INFO trading\.strategy: -+$')

    # Line patterns for various log entries
    patterns = {
        # MCR patterns
        'mcr_gap': re.compile(r'MCR:\s+Gap: (\w+) \(([-+]?[\d.]+)%, atr_pct=([\d.]+)%\)'),
        'mcr_pattern': re.compile(r'MCR:\s+Pattern: (\w+)'),
        'mcr_2day_change': re.compile(r'MCR:\s+2-day change: ([-+]?[\d.]+)%'),
        'mcr_d1': re.compile(r'MCR:\s+D-1: (\w+), closed (\w+)'),
        'mcr_d1_ema39': re.compile(r'MCR:\s+D-1 vs EMA39: (\w+)'),
        'mcr_eod_regime': re.compile(r'MCR:\s+EOD intraday regime: (\w+)'),
        'mcr_trend_strength': re.compile(r'MCR:\s+Trend strength: (\w+)'),
        'mcr_long_signal': re.compile(r'MCR:\s+Long signal: (\w+) \(([\d.]+)\)'),
        'mcr_short_signal': re.compile(r'MCR:\s+Short signal: (\w+) \(([\d.]+)\)'),
        'mcr_daily_trend': re.compile(r'MCR: Daily trend \(EMA8/20\): (\w+)'),

        # Daily patterns
        'daily_opportunity': re.compile(r'Daily: LONG opportunity=([\d.]+)'),
        'daily_exhaustion': re.compile(r'Daily: exhaustion_signal=(\w+)'),
        'daily_trend_strength': re.compile(r'Daily: trend_strength=(\w+)'),
        'daily_risk_multiplier': re.compile(r'Daily: risk_multiplier=([\d.]+)'),

        # TCC patterns
        'tcc_gap': re.compile(r'TCC: gap_bucket=(\w+), gap_pct=([-\d.]+), atr_pct=([\d.]+)'),
        'tcc_vol': re.compile(r'TCC: vol_bucket=(\w+)'),
        'tcc_rsi': re.compile(r'TCC: rsi_bucket=(\w+)'),
        'tcc_ib': re.compile(r'TCC: ib_band=(\w+), iv_pass=(\w+)'),
        'tcc_profile': re.compile(r'TCC: profile_regime=(\w+)'),
        'tcc_prior': re.compile(r'TCC: prior_band=(\w+)'),
        'tcc_composite': re.compile(r'TCC: composite_band=(\w+)'),
        'tcc_poc_drift': re.compile(r'TCC: poc_drift_pct=([-\d.]+)'),
        'tcc_quadrant': re.compile(r'TCC: quadrant=(\w+), gt_pass=(\w+)'),
        'tcc_env': re.compile(r'TCC: env=(\w+)'),
        'tcc_context': re.compile(r'TCC: context_bucket=(\w+)'),
        'tcc_reason': re.compile(r'TCC: reason=(.+)$'),
        'tcc_trend_bias': re.compile(r'TCC: trend_bias=(\w+)'),
        'tcc_intraday_bias': re.compile(r'TCC: intraday_bias=(\w+)'),
        'tcc_ofa_reason': re.compile(r'TCC: ofa_reason=(.+)$'),

        # Entry patterns
        'entry_bucket': re.compile(r'Entry: bucket=(\w+)'),
        'entry_allow': re.compile(r'Entry: allow=(\w+)'),
        'entry_risk': re.compile(r'Entry: risk=\$(\d+)'),

        # STR patterns
        'str_ema39': re.compile(r'STR EMA39_relation: (\w+)'),
        'str_supertrend': re.compile(r'STR SuperTrend: ([-\d]+)'),
        'str_entry': re.compile(r'STR Entry: ([\d.]+)'),
        'str_quality': re.compile(r'STR Trade Score Entry Quality: (\w+)'),
        'str_confidence': re.compile(r'STR Trade Score Confidence: ([\d.]+)'),
        'str_signal_strength': re.compile(r'STR Trade Signal Strength: (\w+)'),
        'str_range_vs_atr': re.compile(r'STR candle range_vs_atr: ([\d.]+)'),
        'str_etf_cum_vol': re.compile(r'STR etf_cum_vol: ([-\d.]+)'),
        'str_cum_vol': re.compile(r'STR cum_vol: ([-\d]+)'),
        'str_normalized_ib': re.compile(r'STR normalized_ib_range: ([\d.]+)'),
        'str_all_conditions': re.compile(r'STR all_conditions_met: (\w+)'),

        # Entry blocked pattern
        'entry_blocked': re.compile(r'0: Entry blocked: (.+)$'),

        # Boost/Reduce patterns
        'boost': re.compile(r'BOOST: (.+)$'),
        'reduce': re.compile(r'REDUCE: (.+)$'),
    }

    def create_empty_entry():
        return {
            'symbol': None,
            'trade_datetime': None,
            # MCR fields
            'gap_type': None,
            'gap_pct': None,
            'atr_pct': None,
            'pattern': None,
            'two_day_change': None,
            'd1_color': None,
            'd1_close': None,
            'd1_vs_ema39': None,
            'eod_regime': None,
            'mcr_trend_strength': None,
            'long_signal': None,
            'long_signal_score': None,
            'short_signal': None,
            'short_signal_score': None,
            'daily_trend_ema': None,
            # Daily fields
            'daily_long_opportunity': None,
            'daily_exhaustion_signal': None,
            'daily_trend_strength': None,
            'daily_risk_multiplier': None,
            # TCC fields
            'gap_bucket': None,
            'tcc_gap_pct': None,
            'tcc_atr_pct': None,
            'vol_bucket': None,
            'rsi_bucket': None,
            'ib_band': None,
            'iv_pass': None,
            'profile_regime': None,
            'prior_band': None,
            'composite_band': None,
            'poc_drift_pct': None,
            'quadrant': None,
            'gt_pass': None,
            'env': None,
            'context_bucket': None,
            'tcc_reason': None,
            'trend_bias': None,
            'intraday_bias': None,
            'ofa_reason': None,
            # Entry fields
            'entry_bucket': None,
            'allow_trade': None,
            'risk_amount': None,
            # STR fields
            'ema39_relation': None,
            'super_trend': None,
            'entry_price': None,
            'entry_quality': None,
            'confidence': None,
            'signal_strength': None,
            'range_vs_atr': None,
            'etf_cum_vol': None,
            'cum_vol': None,
            'normalized_ib_range': None,
            'all_conditions_met': None,
            # Additional
            'entry_blocked_reason': None,
            'boost_reason': None,
            'reduce_reason': None,
        }

    with open(log_path, 'r', encoding='utf-8') as f:
        for line in f:
            # Check for header line (start of new entry)
            header_match = header_pattern.search(line)
            if header_match:
                # Save previous entry if exists
                if current_entry and current_entry['symbol']:
                    entries.append(current_entry)

                # Start new entry
                current_entry = create_empty_entry()
                current_entry['symbol'] = header_match.group(1)
                current_entry['trade_datetime'] = header_match.group(2)
                continue

            if not current_entry:
                continue

            # Skip separator lines
            if separator_pattern.search(line):
                continue

            # MCR patterns
            match = patterns['mcr_gap'].search(line)
            if match:
                current_entry['gap_type'] = match.group(1)
                current_entry['gap_pct'] = float(match.group(2))
                current_entry['atr_pct'] = float(match.group(3))
                continue

            match = patterns['mcr_pattern'].search(line)
            if match:
                current_entry['pattern'] = match.group(1)
                continue

            match = patterns['mcr_2day_change'].search(line)
            if match:
                current_entry['two_day_change'] = float(match.group(1))
                continue

            match = patterns['mcr_d1'].search(line)
            if match:
                current_entry['d1_color'] = match.group(1)
                current_entry['d1_close'] = match.group(2)
                continue

            match = patterns['mcr_d1_ema39'].search(line)
            if match:
                current_entry['d1_vs_ema39'] = match.group(1)
                continue

            match = patterns['mcr_eod_regime'].search(line)
            if match:
                current_entry['eod_regime'] = match.group(1)
                continue

            match = patterns['mcr_trend_strength'].search(line)
            if match:
                current_entry['mcr_trend_strength'] = match.group(1)
                continue

            match = patterns['mcr_long_signal'].search(line)
            if match:
                current_entry['long_signal'] = match.group(1)
                current_entry['long_signal_score'] = float(match.group(2))
                continue

            match = patterns['mcr_short_signal'].search(line)
            if match:
                current_entry['short_signal'] = match.group(1)
                current_entry['short_signal_score'] = float(match.group(2))
                continue

            match = patterns['mcr_daily_trend'].search(line)
            if match:
                current_entry['daily_trend_ema'] = match.group(1)
                continue

            # Daily patterns
            match = patterns['daily_opportunity'].search(line)
            if match:
                current_entry['daily_long_opportunity'] = float(match.group(1))
                continue

            match = patterns['daily_exhaustion'].search(line)
            if match:
                current_entry['daily_exhaustion_signal'] = match.group(1)
                continue

            match = patterns['daily_trend_strength'].search(line)
            if match:
                current_entry['daily_trend_strength'] = match.group(1)
                continue

            match = patterns['daily_risk_multiplier'].search(line)
            if match:
                current_entry['daily_risk_multiplier'] = float(match.group(1))
                continue

            # TCC patterns
            match = patterns['tcc_gap'].search(line)
            if match:
                current_entry['gap_bucket'] = match.group(1)
                current_entry['tcc_gap_pct'] = float(match.group(2))
                current_entry['tcc_atr_pct'] = float(match.group(3))
                continue

            match = patterns['tcc_vol'].search(line)
            if match:
                current_entry['vol_bucket'] = match.group(1)
                continue

            match = patterns['tcc_rsi'].search(line)
            if match:
                current_entry['rsi_bucket'] = match.group(1)
                continue

            match = patterns['tcc_ib'].search(line)
            if match:
                current_entry['ib_band'] = match.group(1)
                current_entry['iv_pass'] = match.group(2) == 'True'
                continue

            match = patterns['tcc_profile'].search(line)
            if match:
                current_entry['profile_regime'] = match.group(1)
                continue

            match = patterns['tcc_prior'].search(line)
            if match:
                current_entry['prior_band'] = match.group(1)
                continue

            match = patterns['tcc_composite'].search(line)
            if match:
                current_entry['composite_band'] = match.group(1)
                continue

            match = patterns['tcc_poc_drift'].search(line)
            if match:
                current_entry['poc_drift_pct'] = float(match.group(1))
                continue

            match = patterns['tcc_quadrant'].search(line)
            if match:
                current_entry['quadrant'] = match.group(1)
                current_entry['gt_pass'] = match.group(2) == 'True'
                continue

            match = patterns['tcc_env'].search(line)
            if match:
                current_entry['env'] = match.group(1)
                continue

            match = patterns['tcc_context'].search(line)
            if match:
                current_entry['context_bucket'] = match.group(1)
                continue

            match = patterns['tcc_reason'].search(line)
            if match:
                current_entry['tcc_reason'] = match.group(1).strip()
                continue

            match = patterns['tcc_trend_bias'].search(line)
            if match:
                current_entry['trend_bias'] = match.group(1)
                continue

            match = patterns['tcc_intraday_bias'].search(line)
            if match:
                current_entry['intraday_bias'] = match.group(1)
                continue

            match = patterns['tcc_ofa_reason'].search(line)
            if match:
                current_entry['ofa_reason'] = match.group(1).strip()
                continue

            # Entry patterns
            match = patterns['entry_bucket'].search(line)
            if match:
                current_entry['entry_bucket'] = match.group(1)
                continue

            match = patterns['entry_allow'].search(line)
            if match:
                current_entry['allow_trade'] = match.group(1) == 'True'
                continue

            match = patterns['entry_risk'].search(line)
            if match:
                current_entry['risk_amount'] = int(match.group(1))
                continue

            # STR patterns
            match = patterns['str_ema39'].search(line)
            if match:
                current_entry['ema39_relation'] = match.group(1)
                continue

            match = patterns['str_supertrend'].search(line)
            if match:
                current_entry['super_trend'] = int(match.group(1))
                continue

            match = patterns['str_entry'].search(line)
            if match:
                current_entry['entry_price'] = float(match.group(1))
                continue

            match = patterns['str_quality'].search(line)
            if match:
                current_entry['entry_quality'] = match.group(1)
                continue

            match = patterns['str_confidence'].search(line)
            if match:
                current_entry['confidence'] = float(match.group(1))
                continue

            match = patterns['str_signal_strength'].search(line)
            if match:
                current_entry['signal_strength'] = match.group(1)
                continue

            match = patterns['str_range_vs_atr'].search(line)
            if match:
                current_entry['range_vs_atr'] = float(match.group(1))
                continue

            match = patterns['str_etf_cum_vol'].search(line)
            if match:
                current_entry['etf_cum_vol'] = float(match.group(1))
                continue

            match = patterns['str_cum_vol'].search(line)
            if match:
                current_entry['cum_vol'] = int(match.group(1))
                continue

            match = patterns['str_normalized_ib'].search(line)
            if match:
                current_entry['normalized_ib_range'] = float(match.group(1))
                continue

            match = patterns['str_all_conditions'].search(line)
            if match:
                current_entry['all_conditions_met'] = match.group(1) == 'True'
                continue

            # Entry blocked
            match = patterns['entry_blocked'].search(line)
            if match:
                current_entry['entry_blocked_reason'] = match.group(1).strip()
                continue

            # Boost/Reduce
            match = patterns['boost'].search(line)
            if match:
                current_entry['boost_reason'] = match.group(1).strip()
                continue

            match = patterns['reduce'].search(line)
            if match:
                current_entry['reduce_reason'] = match.group(1).strip()
                continue

    # Don't forget the last entry
    if current_entry and current_entry['symbol']:
        entries.append(current_entry)

    return entries


def write_csv(entries: list[dict], output_path: str):
    """Write parsed entries to CSV file."""

    if not entries:
        print("No entries found to write.")
        return

    fieldnames = [
        'symbol', 'trade_datetime',
        # Entry fields
        'entry_bucket', 'allow_trade', 'ofa_reason', 'risk_amount',
        # Additional
        'entry_blocked_reason', 'boost_reason', 'reduce_reason',
        # MCR fields
        'gap_type', 'gap_pct', 'atr_pct', 'pattern', 'two_day_change',
        'd1_color', 'd1_close', 'd1_vs_ema39', 'eod_regime', 'mcr_trend_strength',
        'long_signal', 'long_signal_score', 'short_signal', 'short_signal_score',
        'daily_trend_ema',
        # Daily fields
        'daily_long_opportunity', 'daily_exhaustion_signal', 'daily_trend_strength',
        'daily_risk_multiplier',
        # TCC fields
        'gap_bucket', 'tcc_gap_pct', 'tcc_atr_pct', 'vol_bucket', 'rsi_bucket',
        'ib_band', 'iv_pass', 'profile_regime', 'prior_band', 'composite_band',
        'poc_drift_pct', 'quadrant', 'gt_pass', 'env', 'context_bucket',
        'tcc_reason', 'trend_bias', 'intraday_bias',
        # STR fields
        'ema39_relation', 'super_trend', 'entry_price', 'entry_quality',
        'confidence', 'signal_strength', 'range_vs_atr', 'etf_cum_vol',
        'cum_vol', 'normalized_ib_range', 'all_conditions_met',
    ]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(entries)

    print(f"Successfully wrote {len(entries)} entries to {output_path}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python log_to_csv.py <input_log_file> [output_csv_file]")
        print("Example: python log_to_csv.py strategy.log output.csv")
        sys.exit(1)

    input_path = sys.argv[1]

    # Default output path: same name as input but with .csv extension
    if len(sys.argv) >= 3:
        output_path = sys.argv[2]
    else:
        output_path = Path(input_path).stem + '.csv'

    if not Path(input_path).exists():
        print(f"Error: Input file '{input_path}' not found.")
        sys.exit(1)

    print(f"Parsing log file: {input_path}")
    entries = parse_log_file(input_path)
    print(f"Found {len(entries)} entries")

    write_csv(entries, output_path)



def main2(input_path, output_path):

    if not Path(input_path).exists():
        print(f"Error: Input file '{input_path}' not found.")
        sys.exit(1)

    print(f"Parsing log file: {input_path}")
    entries = parse_log_file(input_path)
    print(f"Found {len(entries)} entries")

    write_csv(entries, output_path)

if __name__ == '__main__':
    input_path = "C:/source/TSV2_Dup/strategy_v8_5_baseline.log"
    output_path = "C:/Users/mvbbg/Documents/Sim_Results/v9/strategy_v8_5_baseline.csv"
    main2(input_path, output_path)