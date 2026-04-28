import pandas as pd


def mark_even_minute_intervals(df, metric_id_col, time_col, max_gap_mins=30, min_streak=2):
    df = df.copy()
    # Ensure chronological order
    df = df.sort_values(time_col)
    df['original_index'] = range(len(df))
    
    # IDs we suspect are system-triggered after silence
    AUTO_IDS = ['14030', '14040', '33500']
    
    # 1. PRELIMINARY SYSTEM CHECK
    # Check for Heartbeats
    df['prev_metric'] = df[metric_id_col].shift(1)
    df['is_pure_consecutive'] = df[metric_id_col] == df['prev_metric']
    
    df_sorted = df.sort_values([metric_id_col, time_col])
    df_sorted['delta_sec'] = df_sorted.groupby(metric_id_col)[time_col].diff().dt.total_seconds()
    
    # Heartbeat Math (Even minutes)
    limit_sec = (max_gap_mins * 60) + 1
    df_sorted['is_even'] = (df_sorted['delta_sec'] % 60).isin([0, 1, 59]) & \
                           (df_sorted['delta_sec'] >= 59) & (df_sorted['delta_sec'] <= limit_sec)
    
    df_sorted['is_trusted_candidate'] = df_sorted['is_even'] & df_sorted['is_pure_consecutive']
    group_blocks = (df_sorted['is_trusted_candidate'] != df_sorted.groupby(metric_id_col)['is_trusted_candidate'].shift()).cumsum()
    df_sorted['streak_count'] = df_sorted.groupby([metric_id_col, group_blocks])['is_trusted_candidate'].transform('count')
    df_sorted['is_personality_builder'] = df_sorted['is_trusted_candidate'] & (df_sorted['streak_count'] >= min_streak)

    # Apply Heartbeat Signatures
    def get_signature(group):
        trusted = group.loc[group['is_personality_builder'], 'delta_sec']
        if trusted.empty:
            return None
        return ((trusted / 60).round() * 60).mode().iloc[0]

    signatures = df_sorted.groupby(metric_id_col).apply(get_signature, include_groups=False).rename('signature_sec')
    df = df.join(signatures, on=metric_id_col)
    
    df = df.sort_values([metric_id_col, time_col])
    df['delta_sec'] = df.groupby(metric_id_col)[time_col].diff().dt.total_seconds()
    
    is_heartbeat = (
        (df['delta_sec'] % 60).isin([0, 1, 59]) &
        (df['signature_sec'].notna()) &
        ((df['delta_sec'] - df['signature_sec']).abs() <= 1)
    )

    # 2. SILENCE LOGIC (The "Human Pulse" Tracker)
    # We tag rows that are EITHER heartbeats OR Auto-IDs as "Non-Human" for the silence calculation
    df['is_auto_id'] = df[metric_id_col].astype(str).isin(AUTO_IDS)
    df['is_system_prelim'] = is_heartbeat | df['is_auto_id']
    
    df = df.sort_values(time_col)
    
    # Carry forward the last known timestamp of a row that is DEFINITELY not a system/auto ID
    df['last_human_ts'] = df[time_col].where(~df['is_system_prelim']).ffill().shift(1)
    df['silence_since_human'] = (df[time_col] - df['last_human_ts']).dt.total_seconds()

    # Define Timeout: If it's an Auto-ID and it's been 15+ mins since a TRUE human action
    is_timeout_action = (df['is_auto_id']) & (df['silence_since_human'] > 900)

    # 3. FINAL LABELING
    df['activity_type'] = ''
    df.loc[is_heartbeat | is_timeout_action, 'activity_type'] = 'Possible System Action'
    
    # 4. FORMATTING & CLEANUP
    def format_duration(row):
        if row['activity_type'] == 'Possible System Action':
            sec = row['silence_since_human'] if (row['silence_since_human'] > 900) else row['delta_sec']
            if pd.isna(sec) or sec == 0:
                return ""
            return f"{int(sec // 60):02d}:{int(sec % 60):02d}"
        return ""

    df['Time_Gap_Display'] = df.apply(format_duration, axis=1)
    
    drop_cols = ['original_index', 'prev_metric', 'is_pure_consecutive', 'delta_sec', 
                 'is_auto_id', 'is_system_prelim', 'last_human_ts', 'silence_since_human', 'signature_sec']
    processed_df = df.sort_values('original_index').drop(columns=drop_cols)
    summary_df = processed_df[processed_df['activity_type'] == 'Possible System Action'].groupby(metric_id_col).size().reset_index(name='Total_System_Actions')
    
    return processed_df, summary_df