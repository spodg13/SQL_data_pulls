import pandas as pd


def mark_even_minute_intervals(df, metric_id_col, time_col, max_gap_mins=30, min_streak=2,toi=900):
    df = df.copy()
    # Ensure chronological order
    df = df.sort_values(time_col)
    df['original_index'] = range(len(df))
    
    # IDs we suspect are system-triggered after silence
    AUTO_IDS = ['14020','14030', '14040', '33500']
    
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
    # --- 1. Calculate the Signatures ---
    def get_signature(group):
        trusted = group.loc[group['is_personality_builder'], 'delta_sec']
        if trusted.empty: 
            return None
        return ((trusted / 60).round() * 60).mode().iloc[0]

    # Create a mapping dictionary: {METRIC_ID: signature_value}
    sig_map = df_sorted.groupby(metric_id_col).apply(get_signature, include_groups=False).to_dict()

    # --- 2. Map back to the main dataframe ---
    # .map() looks up the Metric ID in the dictionary and puts the value in 'signature_sec'
    # This is 100% safe from "ACCESS_TIME_x" errors because there is no merge happening.
    df['signature_sec'] = df[metric_id_col].map(sig_map)
    
    # RE-SORT and RE-CALCULATE local gaps
    # This should now work perfectly because 'ACCESS_TIME' is untouched
    df = df.sort_values([metric_id_col, time_col])
    df['delta_sec'] = df.groupby(metric_id_col)[time_col].diff().dt.total_seconds()
    
    # 3. IDENTIFY HEARTBEATS
    is_heartbeat = (
        (df['delta_sec'] % 60).isin([0, 1, 59]) &
        (df['signature_sec'].notna()) &
        ((df['delta_sec'] - df['signature_sec']).abs() <= 1)
        )
    # 4. SILENCE LOGIC
    df['is_auto_id'] = df[metric_id_col].astype(str).isin(AUTO_IDS)
    df['is_system_prelim'] = is_heartbeat | df['is_auto_id']
    
    # Sort chronologically for the silence tracker
    df = df.sort_values(time_col)
    
    # Calculate silence since definitely-human action
    df['last_human_ts'] = df[time_col].where(~df['is_system_prelim']).ffill().shift(1)
    df['silence_since_human'] = (df[time_col] - df['last_human_ts']).dt.total_seconds()

    is_timeout_action = (df['is_auto_id']) & (df['silence_since_human'] > toi)

    # 5. FINAL LABELING
    df['activity_type'] = ''
    df.loc[is_heartbeat, 'activity_type'] = 'Possible System Action'
    # Second, label the inactivity triggers specifically
    # This will overwrite 'Possible System Action' if an event happens to be both,
    # but specifically targets the 14030/14040/33500 group.
    df.loc[is_timeout_action, 'activity_type'] = 'Inactivity-Possible System Action'
    
    # --- NEW SUMMARY ENHANCEMENT (Place here) ---
    # Convert the signature to a readable string (e.g., "5 min")
    # For rows that were inactivity timeouts (no signature), label them as such
    df['Detected_Interval'] = (df['signature_sec'] / 60).fillna(0).astype(int).astype(str) + " min"
    df.loc[is_timeout_action, 'Detected_Interval'] = f'Inactivity > {int(toi//60)}m'
    #

    # 6. FORMATTING & CLEANUP
    def format_duration(row):
        # Checks if 'Possible System Action' is anywhere in the string
        if 'Possible System Action' in row['activity_type']:
            sec = row['silence_since_human'] if (row['silence_since_human'] > toi) else row['delta_sec']
            if pd.isna(sec) or sec == 0:
                return ""
            return f"{int(sec // 60):02d}:{int(sec % 60):02d}"
        return ""

    df['Time_Gap_Display'] = df.apply(format_duration, axis=1)
    
    # Update drop_cols to keep 'Detected_Interval' long enough to build the summary
    drop_cols = ['original_index', 'prev_metric', 'is_pure_consecutive', 'delta_sec', 
                 'is_auto_id', 'is_system_prelim', 'last_human_ts', 'silence_since_human', 'signature_sec']
    
    # Create the processed_df
    processed_df = df.sort_values('original_index').drop(columns=drop_cols)
    
    # Updated summary_df to include the interval
    # Use .str.contains to catch both 'Possible System Action' 
    # AND 'Inactivity-Possible System Action'
    # --- ENHANCED SUMMARY WITH TIMEOUT HEADER ---
    # 6-1. Filter for system actions
    system_actions = processed_df[processed_df['activity_type'].str.contains('Possible System Action', na=False)]

    # 6-2. Build the group-by summary
    summary_df = system_actions.groupby(
        [metric_id_col, 'Detected_Interval', 'activity_type']
    ).size().reset_index(name='Total_System_Actions')
    
    # 6-3. Inject the "Epic Timeout Setting" as the first column for professionalism
    # This makes it clear what threshold was used for the "Inactivity" labels
    summary_df.insert(0, 'User_Timeout_Setting', f"{int(toi // 60)} min")
    
    # Final cleanup: If you don't want 'Detected_Interval' in your main Excel rows, 
    # remove it from processed_df now
    if 'Detected_Interval' in processed_df.columns:
        processed_df = processed_df.drop(columns=['Detected_Interval'])
    
    return processed_df, summary_df
    
