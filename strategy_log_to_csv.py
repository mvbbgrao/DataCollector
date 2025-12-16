#!/usr/bin/env python3
"""
Parse trading strategy V9 log files and convert to CSV format.
Usage: python log_to_csv_v9.py <input_log_file> [output_csv_file]
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

    # Line patterns for various log entries (with numbered prefix like "0: MCR:")
    patterns = {
        # MCR patterns
        'mcr_gap': re.compile(r'\d+: MCR: Gap=(\w+)'),
        'mcr_pattern': re.compile(r'\d+: MCR: Pattern=(\w+)'),
        'mcr_2day_change': re.compile(r'\d+: MCR: 2-day_change: ([-+]?[\d.]+)%'),
        'mcr_d1': re.compile(r'\d+: MCR: D-1=(\w+), closed=(\w+)'),
        'mcr_d1_ema39': re.compile(r'\d+: MCR: d1_vs_EMA39=(\w+)'),
        'mcr_eod_regime': re.compile(r'\d+: MCR: EOD_intraday_regime=(\w+)'),
        'mcr_trend_strength': re.compile(r'\d+: MCR: Trend_strength=(\w+)'),
        'mcr_long_signal': re.compile(r'\d+: MCR: Long_signal=(\w+)'),
        'mcr_short_signal': re.compile(r'\d+: MCR: Short_signal: (\w+)'),
        'mcr_daily_trend': re.compile(r'\d+: MCR: daily_trend=(\w+)'),
        'mcr_exhaustion_signal': re.compile(r'\d+: MCR: ExhaustionSignal=(\w+)'),
        'mcr_long_opportunity': re.compile(r'\d+: MCR: long_opportunity=(\w+)'),
        'mcr_short_opportunity': re.compile(r'\d+: MCR: short_opportunity=(\w+)'),

        # Daily patterns
        'daily_opportunity': re.compile(r'\d+: Daily: LONG opportunity=([\d.]+)'),
        'daily_exhaustion': re.compile(r'\d+: Daily: exhaustion_signal=(\w+)'),
        'daily_trend_strength': re.compile(r'\d+: Daily: trend_strength=(\w+)'),
        'daily_risk_multiplier': re.compile(r'\d+: Daily: risk_multiplier=([\d.]+)'),

        # TCC patterns
        'tcc_gap': re.compile(r'\d+: TCC: gap_bucket=(\w+), gap_pct=([-\d.]+), atr_pct=([\d.]+)'),
        'tcc_vol': re.compile(r'\d+: TCC: vol_bucket=(\w+)'),
        'tcc_rsi': re.compile(r'\d+: TCC: rsi_bucket=(\w+)'),
        'tcc_ib': re.compile(r'\d+: TCC: ib_band=(\w+), iv_pass=(\w+)'),
        'tcc_profile': re.compile(r'\d+: TCC: profile_regime=(\w+)'),
        'tcc_prior': re.compile(r'\d+: TCC: prior_band=(\w+)'),
        'tcc_composite': re.compile(r'\d+: TCC: composite_band=(\w+)'),
        'tcc_poc_drift': re.compile(r'\d+: TCC: poc_drift_pct=([-\d.]+)'),
        'tcc_quadrant': re.compile(r'\d+: TCC: quadrant=(\w+), gt_pass=(\w+)'),
        'tcc_env': re.compile(r'\d+: TCC: env=(\w+)'),
        'tcc_context': re.compile(r'\d+: TCC: context_bucket=(\w+)'),
        'tcc_reason': re.compile(r'\d+: TCC: reason=(.+)$'),
        'tcc_trend_bias': re.compile(r'\d+: TCC: trend_bias=(\w+)'),
        'tcc_intraday_bias': re.compile(r'\d+: TCC: intraday_bias=(\w+)'),
        'tcc_ofa_reason': re.compile(r'\d+: TCC: ofa_reason=(.+)$'),

        # Entry patterns
        'entry_stock_mmr': re.compile(r'\d+: Entry: stock_mmr=(\w+)'),
        'entry_bucket': re.compile(r'\d+: Entry: bucket=(\w+)'),
        'entry_allow': re.compile(r'\d+: Entry: allow=(\w+)'),
        'entry_risk': re.compile(r'\d+: Entry: risk=\$(\d+)'),

        # STR patterns
        'str_rs_filter_pass': re.compile(r'\d+: STR RS_Filter_Pass=(\w+)'),
        'str_profile_width': re.compile(r'\d+: STR profile_width=(\w+)'),
        'str_profile_va_range': re.compile(r'\d+: STR profile_value_area_range=([\d.]+)'),
        'str_ema39': re.compile(r'\d+: STR EMA39_relation=(\w+)'),
        'str_supertrend': re.compile(r'\d+: STR SuperTrend: ([-\d]+)'),
        'str_entry': re.compile(r'\d+: STR Entry=([\d.]+)'),
        'str_quality': re.compile(r'\d+: STR Trade Score Entry Quality=(\w+)'),
        'str_confidence': re.compile(r'\d+: STR Trade Score Confidence=([\d.]+)'),
        'str_signal_strength': re.compile(r'\d+: STR Trade Signal Strength=(\w+)'),
        'str_range_vs_atr': re.compile(r'\d+: STR candle range_vs_atr=([\d.]+)'),
        'str_etf_cum_vol': re.compile(r'\d+: STR etf_cum_vol=([-\d.]+)'),
        'str_cum_vol': re.compile(r'\d+: STR cum_vol=([-\d]+)'),
        'str_normalized_ib': re.compile(r'\d+: STR normalized_ib_range=([\d.]+)'),
        'str_all_conditions': re.compile(r'\d+: STR all_conditions_met=(\w+)'),
        'str_pattern_should_trade': re.compile(r'\d+: STR pattern_should_trade=(\w+)'),
        'str_etf_gate_q1': re.compile(r'\d+: STR ETF Trade Gate=Q1-(\w+)'),
        'str_etf_gate_q2': re.compile(r'\d+: STR ETF Trade Gate=Q2-(\w+)'),
        'str_etf_gate_q3': re.compile(r'\d+: STR ETF Trade Gate=Q3-(\w+)'),
        'str_etf_gate_q4': re.compile(r'\d+: STR ETF Trade Gate=Q4[=-](\w+)'),
        'str_etf_gate_mmr': re.compile(r'\d+: STR ETF Trade Gate=MMR-(\w+)'),
        'str_projected_high': re.compile(r'\d+: STR Projected High=([\d.]+)'),
        'str_stop_loss': re.compile(r'\d+: STR Stop Loss=([\d.]+)'),
        'str_target_price': re.compile(r'\d+: STR Target Price=([\d.]+)'),

        # RS patterns (Relative Strength)
        'rs_strength': re.compile(r'\d+: RS: strength=(\w+)'),
        'rs_direction': re.compile(r'\d+: RS: direction=(\w+)'),
        'rs_composite_score': re.compile(r'\d+: RS: composite_score=([\d.]+)'),
        'rs_passes_count': re.compile(r'\d+: RS: passes_count=(\d+)'),
        'rs_is_actionable': re.compile(r'\d+: RS: is_actionable=(\w+)'),
        'rs_volume_pass': re.compile(r'\d+: RS: volume_pass=(\w+)'),
        'rs_volume_score': re.compile(r'\d+: RS: volume_score=([\d.]+)'),
        'rs_volume_absolute_pass': re.compile(r'\d+: RS: volume_absolute_pass=(\w+)'),
        'rs_volume_absolute_score': re.compile(r'\d+: RS: volume_absolute_score=([\d.]+)'),
        'rs_volume_relative_pass': re.compile(r'\d+: RS: volume_relative_pass=(\w+)'),
        'rs_volume_relative_score': re.compile(r'\d+: RS: volume_relative_score=([\d.]+)'),
        'rs_rvol': re.compile(r'\d+: RS: rvol=([\d.]+)'),
        'rs_etf_rvol': re.compile(r'\d+: RS: etf_rvol=([\d.]+)'),
        'rs_rvol_ratio': re.compile(r'\d+: RS: rvol_ratio=([\d.]+|N/A)'),
        'rs_rvol_source': re.compile(r'\d+: RS: rvol_source=(\w+)'),
        'rs_slot_index': re.compile(r'\d+: RS: slot_index=(\d+|N/A)'),
        'rs_pct_of_day_typical': re.compile(r'\d+: RS: pct_of_day_typical=([\d.]+|N/A)'),
        'rs_trend_pass': re.compile(r'\d+: RS: trend_pass=(\w+)'),
        'rs_trend_score': re.compile(r'\d+: RS: trend_score=([\d.]+)'),
        'rs_stock_vs_vwap': re.compile(r'\d+: RS: stock_vs_vwap=([-\d.]+)'),
        'rs_etf_vs_vwap': re.compile(r'\d+: RS: etf_vs_vwap=([-\d.]+)'),
        'rs_relative_vwap_spread': re.compile(r'\d+: RS: relative_vwap_spread=([-\d.]+)'),
        'rs_vwap_divergent': re.compile(r'\d+: RS: vwap_divergent=(\w+)'),
        'rs_outperform_pass': re.compile(r'\d+: RS: outperform_pass=(\w+)'),
        'rs_outperform_score': re.compile(r'\d+: RS: outperform_score=([\d.]+)'),
        'rs_stock_pct_change': re.compile(r'\d+: RS: stock_pct_change=([-\d.]+)'),
        'rs_etf_pct_change': re.compile(r'\d+: RS: etf_pct_change=([-\d.]+)'),
        'rs_alpha_pct': re.compile(r'\d+: RS: alpha_pct=([-\d.]+)'),
        'rs_outperform_ratio': re.compile(r'\d+: RS: outperform_ratio=([-\d.]+|N/A)'),
        'rs_ema_distance_pass': re.compile(r'\d+: RS: ema_distance_pass=(\w+)'),
        'rs_ema_distance_score': re.compile(r'\d+: RS: ema_distance_score=([\d.]+)'),
        'rs_stock_ema_distance': re.compile(r'\d+: RS: stock_ema_distance=([-\d.]+)'),
        'rs_etf_ema_distance': re.compile(r'\d+: RS: etf_ema_distance=([-\d.]+)'),
        'rs_ema_stretch_ratio': re.compile(r'\d+: RS: ema_stretch_ratio=([\d.]+|N/A)'),

        # Entry blocked pattern
        'entry_blocked': re.compile(r'\d+: IGF Entry blocked: (.+)$'),

        # Boost/Reduce/Block patterns
        'boost': re.compile(r'\d+: BOOST: (.+)$'),
        'reduce': re.compile(r'\d+: REDUCE: (.+)$'),
        'block': re.compile(r'\d+: BLOCK: (.+)$'),

        # ETF_MCR patterns
        'etf_mcr_symbol': re.compile(r'\d+: ETF_MCR: symbol=(\w+)'),
        'etf_mcr_below_ema20': re.compile(r'\d+: ETF_MCR: day_close_below_ema20=(\w+)'),
        'etf_mcr_gap': re.compile(r'\d+: ETF_MCR: gap=(\w+) \(([-+]?[\d.]+)%, atr_pct=([\d.]+)%'),
        'etf_mcr_pattern': re.compile(r'\d+: ETF_MCR: pattern=(\w+), 2d_change=([-+]?[\d.]+)%'),
        'etf_mcr_vol': re.compile(r'\d+: ETF_MCR: etf_vol_bucket=(\w+)'),
        'etf_mcr_intraday_bias': re.compile(r'\d+: ETF_MCR: etf_intraday_bias=(\w+)'),
        'etf_mcr_regime': re.compile(r'\d+: ETF_MCR: intraday=(\w+), daily_trend=(\w+), trend_strength=(\w+)'),
        'etf_mcr_signals': re.compile(
            r'\d+: ETF_MCR: long=(\w+)\(([\d.]+)\), short=(\w+)\(([\d.]+)\), cycle_score=([-+]?[\d.]+), risk_mult=([\d.]+)'),
    }

    def create_empty_entry():
        return {
            'symbol': None,
            'trade_datetime': None,
            # MCR fields
            'gap_type': None,
            'pattern': None,
            'two_day_change': None,
            'd1_color': None,
            'd1_close': None,
            'd1_vs_ema39': None,
            'eod_regime': None,
            'mcr_trend_strength': None,
            'long_signal': None,
            'short_signal': None,
            'exhaustion_signal': None,
            'long_opportunity': None,
            'short_opportunity': None,
            'mcr_daily_trend': None,
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
            'stock_mmr': None,
            'entry_bucket': None,
            'allow_trade': None,
            'risk_amount': None,
            # RS fields
            'rs_filter_pass': None,
            'rs_strength': None,
            'rs_direction': None,
            'rs_composite_score': None,
            'rs_passes_count': None,
            'rs_is_actionable': None,
            'rs_volume_pass': None,
            'rs_volume_score': None,
            'rs_volume_absolute_pass': None,
            'rs_volume_absolute_score': None,
            'rs_volume_relative_pass': None,
            'rs_volume_relative_score': None,
            'rs_rvol': None,
            'rs_etf_rvol': None,
            'rs_rvol_ratio': None,
            'rs_rvol_source': None,
            'rs_slot_index': None,
            'rs_pct_of_day_typical': None,
            'rs_trend_pass': None,
            'rs_trend_score': None,
            'rs_stock_vs_vwap': None,
            'rs_etf_vs_vwap': None,
            'rs_relative_vwap_spread': None,
            'rs_vwap_divergent': None,
            'rs_outperform_pass': None,
            'rs_outperform_score': None,
            'rs_stock_pct_change': None,
            'rs_etf_pct_change': None,
            'rs_alpha_pct': None,
            'rs_outperform_ratio': None,
            'rs_ema_distance_pass': None,
            'rs_ema_distance_score': None,
            'rs_stock_ema_distance': None,
            'rs_etf_ema_distance': None,
            'rs_ema_stretch_ratio': None,
            # STR fields
            'profile_width': None,
            'profile_value_area_range': None,
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
            'pattern_should_trade': None,
            'etf_gate_q1': None,
            'etf_gate_q2': None,
            'etf_gate_q3': None,
            'etf_gate_q4': None,
            'etf_gate_mmr': None,
            'projected_high': None,
            'stop_loss': None,
            'target_price': None,
            # Additional
            'entry_blocked_reason': None,
            'boost_reason': None,
            'reduce_reason': None,
            'block_reason': None,
            # ETF_MCR fields
            'etf_symbol': None,
            'etf_below_ema20': None,
            'etf_gap_type': None,
            'etf_gap_pct': None,
            'etf_atr_pct': None,
            'etf_pattern': None,
            'etf_2d_change': None,
            'etf_vol_bucket': None,
            'etf_intraday_bias': None,
            'etf_intraday': None,
            'etf_daily_trend': None,
            'etf_trend_strength': None,
            'etf_long_signal': None,
            'etf_long_score': None,
            'etf_short_signal': None,
            'etf_short_score': None,
            'etf_cycle_score': None,
            'etf_risk_mult': None,
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
                continue

            match = patterns['mcr_short_signal'].search(line)
            if match:
                current_entry['short_signal'] = match.group(1)
                continue

            match = patterns['mcr_daily_trend'].search(line)
            if match:
                current_entry['mcr_daily_trend'] = match.group(1)
                continue

            match = patterns['mcr_exhaustion_signal'].search(line)
            if match:
                current_entry['exhaustion_signal'] = match.group(1)
                continue

            match = patterns['mcr_long_opportunity'].search(line)
            if match:
                current_entry['long_opportunity'] = match.group(1)
                continue

            match = patterns['mcr_short_opportunity'].search(line)
            if match:
                current_entry['short_opportunity'] = match.group(1)
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
            match = patterns['entry_stock_mmr'].search(line)
            if match:
                current_entry['stock_mmr'] = match.group(1)
                continue

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

            # RS patterns
            match = patterns['str_rs_filter_pass'].search(line)
            if match:
                current_entry['rs_filter_pass'] = match.group(1) == 'True'
                continue

            match = patterns['rs_strength'].search(line)
            if match:
                current_entry['rs_strength'] = match.group(1)
                continue

            match = patterns['rs_direction'].search(line)
            if match:
                current_entry['rs_direction'] = match.group(1)
                continue

            match = patterns['rs_composite_score'].search(line)
            if match:
                current_entry['rs_composite_score'] = float(match.group(1))
                continue

            match = patterns['rs_passes_count'].search(line)
            if match:
                current_entry['rs_passes_count'] = int(match.group(1))
                continue

            match = patterns['rs_is_actionable'].search(line)
            if match:
                current_entry['rs_is_actionable'] = match.group(1) == 'True'
                continue

            match = patterns['rs_volume_pass'].search(line)
            if match:
                current_entry['rs_volume_pass'] = match.group(1) == 'True'
                continue

            match = patterns['rs_volume_score'].search(line)
            if match:
                current_entry['rs_volume_score'] = float(match.group(1))
                continue

            match = patterns['rs_volume_absolute_pass'].search(line)
            if match:
                current_entry['rs_volume_absolute_pass'] = match.group(1) == 'True'
                continue

            match = patterns['rs_volume_absolute_score'].search(line)
            if match:
                current_entry['rs_volume_absolute_score'] = float(match.group(1))
                continue

            match = patterns['rs_volume_relative_pass'].search(line)
            if match:
                current_entry['rs_volume_relative_pass'] = match.group(1) == 'True'
                continue

            match = patterns['rs_volume_relative_score'].search(line)
            if match:
                current_entry['rs_volume_relative_score'] = float(match.group(1))
                continue

            match = patterns['rs_rvol'].search(line)
            if match:
                current_entry['rs_rvol'] = float(match.group(1))
                continue

            match = patterns['rs_etf_rvol'].search(line)
            if match:
                current_entry['rs_etf_rvol'] = float(match.group(1))
                continue

            match = patterns['rs_rvol_ratio'].search(line)
            if match:
                val = match.group(1)
                current_entry['rs_rvol_ratio'] = None if val == 'N/A' else float(val)
                continue

            match = patterns['rs_rvol_source'].search(line)
            if match:
                current_entry['rs_rvol_source'] = match.group(1)
                continue

            match = patterns['rs_slot_index'].search(line)
            if match:
                val = match.group(1)
                current_entry['rs_slot_index'] = None if val == 'N/A' else int(val)
                continue

            match = patterns['rs_pct_of_day_typical'].search(line)
            if match:
                val = match.group(1)
                current_entry['rs_pct_of_day_typical'] = None if val == 'N/A' else float(val)
                continue

            match = patterns['rs_trend_pass'].search(line)
            if match:
                current_entry['rs_trend_pass'] = match.group(1) == 'True'
                continue

            match = patterns['rs_trend_score'].search(line)
            if match:
                current_entry['rs_trend_score'] = float(match.group(1))
                continue

            match = patterns['rs_stock_vs_vwap'].search(line)
            if match:
                current_entry['rs_stock_vs_vwap'] = float(match.group(1))
                continue

            match = patterns['rs_etf_vs_vwap'].search(line)
            if match:
                current_entry['rs_etf_vs_vwap'] = float(match.group(1))
                continue

            match = patterns['rs_relative_vwap_spread'].search(line)
            if match:
                current_entry['rs_relative_vwap_spread'] = float(match.group(1))
                continue

            match = patterns['rs_vwap_divergent'].search(line)
            if match:
                current_entry['rs_vwap_divergent'] = match.group(1) == 'True'
                continue

            match = patterns['rs_outperform_pass'].search(line)
            if match:
                current_entry['rs_outperform_pass'] = match.group(1) == 'True'
                continue

            match = patterns['rs_outperform_score'].search(line)
            if match:
                current_entry['rs_outperform_score'] = float(match.group(1))
                continue

            match = patterns['rs_stock_pct_change'].search(line)
            if match:
                current_entry['rs_stock_pct_change'] = float(match.group(1))
                continue

            match = patterns['rs_etf_pct_change'].search(line)
            if match:
                current_entry['rs_etf_pct_change'] = float(match.group(1))
                continue

            match = patterns['rs_alpha_pct'].search(line)
            if match:
                current_entry['rs_alpha_pct'] = float(match.group(1))
                continue

            match = patterns['rs_outperform_ratio'].search(line)
            if match:
                val = match.group(1)
                current_entry['rs_outperform_ratio'] = None if val == 'N/A' else float(val)
                continue

            match = patterns['rs_ema_distance_pass'].search(line)
            if match:
                current_entry['rs_ema_distance_pass'] = match.group(1) == 'True'
                continue

            match = patterns['rs_ema_distance_score'].search(line)
            if match:
                current_entry['rs_ema_distance_score'] = float(match.group(1))
                continue

            match = patterns['rs_stock_ema_distance'].search(line)
            if match:
                current_entry['rs_stock_ema_distance'] = float(match.group(1))
                continue

            match = patterns['rs_etf_ema_distance'].search(line)
            if match:
                current_entry['rs_etf_ema_distance'] = float(match.group(1))
                continue

            match = patterns['rs_ema_stretch_ratio'].search(line)
            if match:
                val = match.group(1)
                current_entry['rs_ema_stretch_ratio'] = None if val == 'N/A' else float(val)
                continue

            # STR patterns
            match = patterns['str_profile_width'].search(line)
            if match:
                current_entry['profile_width'] = match.group(1)
                continue

            match = patterns['str_profile_va_range'].search(line)
            if match:
                current_entry['profile_value_area_range'] = float(match.group(1))
                continue

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

            match = patterns['str_pattern_should_trade'].search(line)
            if match:
                current_entry['pattern_should_trade'] = match.group(1) == 'True'
                continue

            match = patterns['str_etf_gate_q1'].search(line)
            if match:
                current_entry['etf_gate_q1'] = match.group(1) == 'True'
                continue

            match = patterns['str_etf_gate_q2'].search(line)
            if match:
                current_entry['etf_gate_q2'] = match.group(1) == 'True'
                continue

            match = patterns['str_etf_gate_q3'].search(line)
            if match:
                current_entry['etf_gate_q3'] = match.group(1) == 'True'
                continue

            match = patterns['str_etf_gate_q4'].search(line)
            if match:
                current_entry['etf_gate_q4'] = match.group(1) == 'True'
                continue

            match = patterns['str_etf_gate_mmr'].search(line)
            if match:
                current_entry['etf_gate_mmr'] = match.group(1)
                continue

            match = patterns['str_projected_high'].search(line)
            if match:
                current_entry['projected_high'] = float(match.group(1))
                continue

            match = patterns['str_stop_loss'].search(line)
            if match:
                current_entry['stop_loss'] = float(match.group(1))
                continue

            match = patterns['str_target_price'].search(line)
            if match:
                current_entry['target_price'] = float(match.group(1))
                continue

            # Entry blocked
            match = patterns['entry_blocked'].search(line)
            if match:
                current_entry['entry_blocked_reason'] = match.group(1).strip()
                continue

            # Boost/Reduce/Block
            match = patterns['boost'].search(line)
            if match:
                current_entry['boost_reason'] = match.group(1).strip()
                continue

            match = patterns['reduce'].search(line)
            if match:
                current_entry['reduce_reason'] = match.group(1).strip()
                continue

            match = patterns['block'].search(line)
            if match:
                current_entry['block_reason'] = match.group(1).strip()
                continue

            # ETF_MCR patterns
            match = patterns['etf_mcr_symbol'].search(line)
            if match:
                current_entry['etf_symbol'] = match.group(1)
                continue

            match = patterns['etf_mcr_below_ema20'].search(line)
            if match:
                current_entry['etf_below_ema20'] = match.group(1) == 'True'
                continue

            match = patterns['etf_mcr_gap'].search(line)
            if match:
                current_entry['etf_gap_type'] = match.group(1)
                current_entry['etf_gap_pct'] = float(match.group(2))
                current_entry['etf_atr_pct'] = float(match.group(3))
                continue

            match = patterns['etf_mcr_pattern'].search(line)
            if match:
                current_entry['etf_pattern'] = match.group(1)
                current_entry['etf_2d_change'] = float(match.group(2))
                continue

            match = patterns['etf_mcr_vol'].search(line)
            if match:
                current_entry['etf_vol_bucket'] = match.group(1)
                continue

            match = patterns['etf_mcr_intraday_bias'].search(line)
            if match:
                current_entry['etf_intraday_bias'] = match.group(1)
                continue

            match = patterns['etf_mcr_regime'].search(line)
            if match:
                current_entry['etf_intraday'] = match.group(1)
                current_entry['etf_daily_trend'] = match.group(2)
                current_entry['etf_trend_strength'] = match.group(3)
                continue

            match = patterns['etf_mcr_signals'].search(line)
            if match:
                current_entry['etf_long_signal'] = match.group(1)
                current_entry['etf_long_score'] = float(match.group(2))
                current_entry['etf_short_signal'] = match.group(3)
                current_entry['etf_short_score'] = float(match.group(4))
                current_entry['etf_cycle_score'] = float(match.group(5))
                current_entry['etf_risk_mult'] = float(match.group(6))
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
        'entry_bucket', 'allow_trade', 'stock_mmr', 'ofa_reason', 'risk_amount',
        # Additional
        'entry_blocked_reason', 'boost_reason', 'reduce_reason', 'block_reason',
        # MCR fields
        'gap_type', 'pattern', 'two_day_change',
        'd1_color', 'd1_close', 'd1_vs_ema39', 'eod_regime', 'mcr_trend_strength',
        'long_signal', 'short_signal', 'exhaustion_signal',
        'long_opportunity', 'short_opportunity', 'mcr_daily_trend',
        # Daily fields
        'daily_long_opportunity', 'daily_exhaustion_signal', 'daily_trend_strength',
        'daily_risk_multiplier',
        # TCC fields
        'gap_bucket', 'tcc_gap_pct', 'tcc_atr_pct', 'vol_bucket', 'rsi_bucket',
        'ib_band', 'iv_pass', 'profile_regime', 'prior_band', 'composite_band',
        'poc_drift_pct', 'quadrant', 'gt_pass', 'env', 'context_bucket',
        'tcc_reason', 'trend_bias', 'intraday_bias',
        # RS fields
        'rs_filter_pass', 'rs_strength', 'rs_direction', 'rs_composite_score',
        'rs_passes_count', 'rs_is_actionable',
        'rs_volume_pass', 'rs_volume_score', 'rs_volume_absolute_pass',
        'rs_volume_absolute_score', 'rs_volume_relative_pass', 'rs_volume_relative_score',
        'rs_rvol', 'rs_etf_rvol', 'rs_rvol_ratio', 'rs_rvol_source',
        'rs_slot_index', 'rs_pct_of_day_typical',
        'rs_trend_pass', 'rs_trend_score', 'rs_stock_vs_vwap', 'rs_etf_vs_vwap',
        'rs_relative_vwap_spread', 'rs_vwap_divergent',
        'rs_outperform_pass', 'rs_outperform_score', 'rs_stock_pct_change',
        'rs_etf_pct_change', 'rs_alpha_pct', 'rs_outperform_ratio',
        'rs_ema_distance_pass', 'rs_ema_distance_score', 'rs_stock_ema_distance',
        'rs_etf_ema_distance', 'rs_ema_stretch_ratio',
        # STR fields
        'profile_width', 'profile_value_area_range',
        'ema39_relation', 'super_trend', 'entry_price', 'entry_quality',
        'confidence', 'signal_strength', 'range_vs_atr', 'etf_cum_vol',
        'cum_vol', 'normalized_ib_range', 'all_conditions_met',
        'pattern_should_trade', 'etf_gate_q1', 'etf_gate_q2', 'etf_gate_q3', 'etf_gate_q4',
        'etf_gate_mmr', 'projected_high', 'stop_loss', 'target_price',
        # ETF_MCR fields
        'etf_symbol', 'etf_below_ema20', 'etf_gap_type', 'etf_gap_pct', 'etf_atr_pct',
        'etf_pattern', 'etf_2d_change', 'etf_vol_bucket', 'etf_intraday_bias',
        'etf_intraday', 'etf_daily_trend', 'etf_trend_strength',
        'etf_long_signal', 'etf_long_score', 'etf_short_signal', 'etf_short_score',
        'etf_cycle_score', 'etf_risk_mult',
    ]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(entries)

    print(f"Successfully wrote {len(entries)} entries to {output_path}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python log_to_csv_v9.py <input_log_file> [output_csv_file]")
        print("Example: python log_to_csv_v9.py strategy.log output.csv")
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
    input_path = "C:/source/TSV2_Dup/logs/strategy_v10.log"
    output_path = "C:/Users/mvbbg/Documents/Sim_Results/v10/strategy_v10_baseline.csv"

    main2(input_path, output_path)