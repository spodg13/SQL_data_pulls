import pandas as pd


def mark_even_minute_intervals(df, metric_id_col, time_col, max_gap_mins=30, min_streak=2):
    df = df.copy()
    
    # 1. Capture original chronological order
    df = df.sort_values(time_col)
    df['original_index'] = range(len(df))
    
    # 2. Identify "Back-to-Back" Purity (Was the previous log entry the same metric?)
    df['prev_metric'] = df[metric_id_col].shift(1)
    df['is_pure_consecutive'] = df[metric_id_col] == df['prev_metric']
    
    # 3. Sort for Grouped Calculation
    df = df.sort_values([metric_id_col, time_col])
    df['delta_sec'] = df.groupby(metric_id_col)[time_col].diff().dt.total_seconds()
    
    # 4. Identify Potential Even-Minute Gaps (with 1s drift tolerance)
    limit_sec = (max_gap_mins * 60) + 1
    df['is_even'] = (df['delta_sec'] % 60).isin([0, 1, 59]) & \
                    (df['delta_sec'] >= 59) & (df['delta_sec'] <= limit_sec)
    
    # 5. Define "Trusted Gaps" (Math is right AND no other metrics intervened)
    df['is_trusted_candidate'] = df['is_even'] & df['is_pure_consecutive']
    
    # 6. Streak Validation on Pure Gaps
    # We count consecutive 'True' values for the trusted candidate check
    group_blocks = (df['is_trusted_candidate'] != df.groupby(metric_id_col)['is_trusted_candidate'].shift()).cumsum()
    df['streak_count'] = df.groupby([metric_id_col, group_blocks])['is_trusted_candidate'].transform('count')
    df['is_personality_builder'] = df['is_trusted_candidate'] & (df['streak_count'] >= min_streak)

    # 7. Determine Global Signature (The "Personality" of the Metric)
    def get_signature(group):
        trusted = group.loc[group['is_personality_builder'], 'delta_sec']
        if trusted.empty:
            return None
        # Round to nearest 60s to find the core heartbeat (5:00, 10:00, etc)
        return ((trusted / 60).round() * 60).mode().iloc[0]

    signatures = df.groupby(metric_id_col).apply(get_signature, include_groups=False).rename('signature_sec')
    df = df.join(signatures, on=metric_id_col)

    # 8. Final Classification
    # Even if interrupted, we mark it if it fits the learned signature
    is_system = (
        df['is_even'] & 
        (df['signature_sec'].notna()) &
        ((df['delta_sec'] - df['signature_sec']).abs() <= 1)
    )

    df['activity_type'] = 'Human/Initial'
    df.loc[is_system, 'activity_type'] = 'Possible System Refresh'
    
    # 9. Create Formatting for Time Gaps
    def format_duration(sec):
        if pd.isna(sec) or sec == 0:
            return ""
        return f"{int(sec // 60):02d}:{int(sec % 60):02d}"

    # Apply the mask: only show the gap if it was categorized as a System Refresh
    df['Time_Gap_Display'] = df.apply(
        lambda x: format_duration(x['delta_sec']) if x['activity_type'] == 'Possible System Refresh' else "", axis=1
    )
    
    # 10. Generate Summary DataFrame for the second sheet
    summary_df = df[df['activity_type'] == 'Possible System Refresh'].groupby(metric_id_col).agg(
        Established_Rate=('delta_sec', lambda x: format_duration(((x / 60).round() * 60).mode().iloc[0])),
        Total_Refreshes=('activity_type', 'count')
    ).reset_index()

    # 11. Cleanup helper columns and restore sort
    drop_cols = ['original_index', 'prev_metric', 'is_pure_consecutive', 'delta_sec', 
                 'is_even', 'is_trusted_candidate', 'streak_count', 'is_personality_builder', 'signature_sec']
    processed_df = df.sort_values('original_index').drop(columns=drop_cols)
    
    return processed_df, summary_df