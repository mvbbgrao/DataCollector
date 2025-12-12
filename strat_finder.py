import pandas as pd

# Load and Merge
trades_df = pd.read_csv('C:/Users/mvbbg/Documents/Sim_Results/v9/v_9_baseline_until0702_trades.csv')
signals_df = pd.read_csv('C:/Users/mvbbg/Documents/Sim_Results/v9/strategy_v9_baseline_until0702_scrub.csv')
# Merge Data
trades_df['datetime_str'] = trades_df['simulation_date'] + ' ' + trades_df['entry_time']
trades_df['datetime'] = pd.to_datetime(trades_df['datetime_str'])
signals_df['datetime'] = pd.to_datetime(signals_df['trade_datetime'])

merged_df = pd.merge(trades_df, signals_df, on=['symbol', 'datetime'], how='inner')
merged_df['is_win'] = merged_df['pnl'] > 0
merged_df['month_str'] = merged_df['datetime'].dt.to_period('M').astype(str)

# Define Features to Exclude
exclude_cols = ['job_number', 'last_trading_day', 'simulation_date', 'ticker_file', 'trade_id',
                'sector', 'symbol', 'entry_time', 'entry_price_x', 'stop', 'exit_time', 'exit_price',
                'pnl', 'size', 'reason', 'datetime', 'datetime_str', 'is_win', 'entry_price_y', 'trade_datetime',
                'target_price', 'stop_loss', 'projected_high', 'confidence', 'month', 'month_str']

# Define Drivers
drivers = ['etf_daily_trend', 'intraday_bias', 'etf_symbol', 'gap_type', 'etf_gate_mmr', 'vol_bucket']

# ---------------------------------------------------------
# 2. LOOP THROUGH TARGET MONTHS
# ---------------------------------------------------------
target_months = ['2025-02', '2025-03', '2025-04', '2025-05', '2025-06']

for target_month in target_months:
    print(f"Processing {target_month}...")

    # Filter for the specific month
    df_month = merged_df[merged_df['month_str'] == target_month].copy()

    if df_month.empty:
        print(f"No data found for {target_month}, skipping.")
        continue

    # Dynamic Feature Definitions (in case columns change slightly)
    feature_cols = [c for c in df_month.columns if c not in exclude_cols]
    secondary = [c for c in feature_cols if
                 c not in drivers and (df_month[c].dtype == 'object' or df_month[c].dtype == 'bool')]

    strategies = []


    # Helper function to register strategies
    def register_strategy(df, mask, name):
        subset = df[mask]
        count = len(subset)
        if count >= 10:  # Minimum trades to be considered a strategy
            wr = subset['is_win'].mean()
            if wr >= 0.60:  # Minimum Win Rate
                strategies.append({
                    'name': name,
                    'indices': list(subset.index),
                    'wr': wr
                })


    # --- STRATEGY MINING LOGIC ---

    # 1. Single Factors
    for col in feature_cols:
        if df_month[col].dtype == 'object' or df_month[col].dtype == 'bool':
            for val in df_month[col].unique():
                mask = df_month[col] == val
                register_strategy(df_month, mask, f"{col} == {val}")

    # 2. Combos: Driver + Driver
    for i in range(len(drivers)):
        for j in range(i + 1, len(drivers)):
            col1, col2 = drivers[i], drivers[j]
            if col1 in df_month.columns and col2 in df_month.columns:
                pairs = df_month[[col1, col2]].drop_duplicates().values
                for val1, val2 in pairs:
                    mask = (df_month[col1] == val1) & (df_month[col2] == val2)
                    register_strategy(df_month, mask, f"{col1} == {val1} AND {col2} == {val2}")

    # 3. Combos: Driver + Secondary
    for col1 in drivers:
        for col2 in secondary:
            if col1 in df_month.columns and col2 in df_month.columns:
                pairs = df_month[[col1, col2]].drop_duplicates().values
                for val1, val2 in pairs:
                    mask = (df_month[col1] == val1) & (df_month[col2] == val2)
                    register_strategy(df_month, mask, f"{col1} == {val1} AND {col2} == {val2}")

    # # Sort strategies by Win Rate
    # strategies.sort(key=lambda x: x['wr'], reverse=True)
    #
    # # Keep Top 50 strategies for this month
    # top_strategies = strategies[:50]
    #
    # # Map Trade ID -> List of Strategies
    # trade_strategies = {}
    # for s in top_strategies:
    #     strat_name = s['name']
    #     for idx in s['indices']:
    #         if idx not in trade_strategies:
    #             trade_strategies[idx] = []
    #         trade_strategies[idx].append(strat_name)
    # -------------------------------------------------------------------------
    # SMART OPTIMIZATION: Ensure Combined Win Rate > 60%
    # -------------------------------------------------------------------------

    final_trade_indices = set()
    selected_strategies = []

    # We will add strategies one by one, starting from the best.
    # We check the cumulative Win Rate as we go.

    for strategy in strategies:
        # 1. Temporarily add the new strategy's trades to our pile
        temp_indices = final_trade_indices.union(set(strategy['indices']))

        # 2. Calculate what the Win Rate would be
        if len(temp_indices) > 0:
            temp_trades = df_month.loc[list(temp_indices)]
            temp_wr = temp_trades['is_win'].mean()

            # 3. LOGIC:
            # If adding this strategy keeps us above 60% (or very close), keep it.
            # If it drags us down too much, skip it.
            # (Here we set a hard floor of 0.60 for the cumulative result)

            if temp_wr >= 0.60:
                final_trade_indices = temp_indices
                selected_strategies.append(strategy)
            else:
                # Optional: You can print which strategy was rejected
                # print(f"Skipping {strategy['name']} (would drop WR to {temp_wr:.2%})")
                pass

    # -------------------------------------------------------------------------
    # BUILD OUTPUT
    # -------------------------------------------------------------------------

    # Map Trade ID -> List of Strategies (Only for the selected ones)
    trade_strategies = {}
    for s in selected_strategies:
        strat_name = s['name']
        strat_wr = s['wr'] * 100  # Store the strategy's specific WR too
        for idx in s['indices']:
            if idx not in trade_strategies:
                trade_strategies[idx] = []
            # Add name AND its individual WR for clarity
            trade_strategies[idx].append(f"{strat_name} ({strat_wr:.0f}%)")
    # Build Final DataFrame for this Month
    final_trades = []
    for idx, strat_list in trade_strategies.items():
        row = df_month.loc[idx]

        trade_data = {
            'simulation_date': row['simulation_date'],
            'trade_id': row['trade_id'],
            'symbol': row['symbol'],
            'bucket': row['entry_bucket'] if 'entry_bucket' in row else "N/A",
            'pnl': row['pnl'],
            'win or loss': "Win" if row['pnl'] > 0 else "Loss",
            'list of strategies matched': "; ".join(strat_list)
        }

        # Add Driver Columns
        for driver in drivers:
            trade_data[driver] = row[driver] if driver in row else "N/A"

        final_trades.append(trade_data)

    results_df = pd.DataFrame(final_trades)

    if not results_df.empty:
        results_df = results_df.sort_values(['simulation_date', 'symbol'])

        total_pnl = results_df['pnl'].sum()
        win_rate = (results_df['pnl'] > 0).mean() * 100

        print(f"--- RESULTS FOR {target_month} ---")
        print(f"Total Trades: {len(results_df)}")
        print(f"Total PnL:    ${total_pnl:,.2f}")
        print(f"Win Rate:     {win_rate:.1f}%")
        # Save unique CSV for each month
        filename = f'optimized_trades_{target_month}.csv'
        results_df.to_csv(filename, index=False)
        print(f"Saved {filename} with {len(results_df)} trades.")
    else:
        print(f"No high-win-rate strategies found for {target_month}.")

print("All months processed.")