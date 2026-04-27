import os
import pyodbc
import pandas as pd
import time as stopwatch
from finished import finished_sound
import refresher_tools as rt
from datetime import datetime, timedelta, time as dt_time
from tkinter import Tk, filedialog, simpledialog, messagebox, StringVar, OptionMenu, Button
from AutoQuery_ArchiveReady import queries  # import your dictionary of queries
import re



# ----------------------------
# CONFIGURATION
# ----------------------------
CHUNK_SIZE_MONTHS = 2


# ----------------------------
# DATABASE CONNECTION
# ----------------------------
def get_connection(anyserver, anydatabase):
    """Return a pyodbc connection to SQL Server using Windows Authentication."""
    conn_str = (
        r"DRIVER={ODBC Driver 17 for SQL Server};"
        r"SERVER=" + anyserver + ";"
        r"DATABASE=" + anydatabase + ";"
        r"Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


# ----------------------------
# USER ID LOOKUP
# ----------------------------
def get_user_id(conn, user_login):
    """Look up user_id by system login."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT USER_ID FROM clarity_rpt..clarity_emp WHERE SYSTEM_LOGIN = ?", 
        (user_login,)
    )
    row = cursor.fetchone()
    return row[0] if row else None


# ----------------------------
# WHERE CLAUSE BUILDER
# ----------------------------
def build_where_clause(filter_type, patient_id=None, user_login=None):
    where_parts = []
    if filter_type == "p" and patient_id:
        where_parts.append("AND a.PAT_ID = @PatientID")
    elif filter_type == "u" and user_login:
        where_parts.append("AND a.USER_ID = @UserID")
    elif filter_type == "b" and patient_id and user_login:
        where_parts.append("AND a.USER_ID = @UserID AND a.PAT_ID = @PatientID")
    return "\n    " + " ".join(where_parts) if where_parts else ""

def get_archive_cutoff():
    today = datetime.today()
    archive_year = today.year - 3
    server = "prd-clarity.et0278.epichosted.com"
    database = "CLARITY_ARCHIVE"
    table = f"CLARITY_ARCHIVE.dbo.ACCESS_LOG_{archive_year}"

    print(f"Determining archive cutoff from {table}")
    conn = get_connection(server, database)

    query = f"""
        SELECT MAX(ACCESS_TIME) AS max_access_time
        FROM {table}
    """

    df = run_query_pyodbc_conn(conn, query)
    conn.close()

    if df.empty or pd.isna(df.loc[0, "max_access_time"]):
        raise RuntimeError(f"Could not determine archive cutoff from {table}")

    max_access_time = df.loc[0, "max_access_time"]
    live_start = max_access_time 


    # Last valid archive record
    archive_end = live_start - timedelta(milliseconds=1)

    print(f"Archive data ends at: {archive_end}")
    print(f"Live data begins at: {live_start}")

    return archive_end, live_start
    

def resolve_tables(chunk_start,live_start):
    
    if chunk_start < live_start:
        year = chunk_start.year
        return {
            "source": "archive",
            "year": chunk_start.year,
            "server": "prd-clarity.et0278.epichosted.com",
            "database": "CLARITY_ARCHIVE",
            "access_log": f"CLARITY_ARCHIVE.dbo.ACCESS_LOG_{year}",
            "acc_log_dtl": f"CLARITY_ARCHIVE.dbo.ACC_LOG_DTL_IX_{year}",
            "acc_log_MTDTL": f"CLARITY_ARCHIVE.dbo.ACC_LOG_MTLDTL_IX_{year}",
            "acc_WRKF": f"CLARITY_ARCHIVE.dbo.ACCESS_WRKF_{year}"
        }
    else:
        return {
            "source": "live",
            "year": None,
            "server": "EDCLM1",
            "database": "clarity_rpt",
            "access_log": "clarity_rpt.dbo.ACCESS_LOG",
            "acc_log_dtl": "clarity_rpt.dbo.ACC_LOG_DTL_IX",
            "acc_log_MTDTL": "clarity_rpt.dbo.ACC_LOG_MTLDTL_IX",
            "acc_WRKF": "clarity_rpt.dbo.ACCESS_WRKF"
        }
# ----------------------------
# CHUNKED DATE RANGE
# ----------------------------
def get_date_ranges(start_date, end_date, archive_end):
    current = start_date

    while current <= end_date:
        next_chunk_proposal = datetime.combine(
            (current + timedelta(days=CHUNK_SIZE_MONTHS * 30)).date(), 
            dt_time.max
        )
        
        # Hard stop at end of the var-current year
        year_end = datetime.combine(datetime(current.year, 12, 31), dt_time.max)
        chunk_end = min(next_chunk_proposal, year_end, end_date)
        # --- NEW: prevent crossing archive boundary ---
        if current <= archive_end < chunk_end:
            chunk_end = archive_end 

        yield current, chunk_end

        # Move one millisecond past chunk_end to avoid infinite loop
        current = (chunk_end + timedelta(seconds=1)).replace(microsecond=0)


# ----------------------------
# QUERY PICKER
# ----------------------------
def pick_query_type(options):
    """Tkinter dropdown for selecting query."""
    root = Tk()
    root.title("Select Query Type")
    selected = StringVar(root)
    selected.set(options[0])

    drop = OptionMenu(root, selected, *options)
    drop.pack(padx=20, pady=10)
    Button(root, text="OK", command=root.quit).pack(pady=10)
    root.mainloop()
    choice = selected.get()
    root.destroy()
    return choice

def run_query_pyodbc_conn(conn, query_text):
    """
    Run a SQL query using an existing pyodbc connection and return a pandas DataFrame.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query_text)

        # Fetch columns
        columns = [col[0] for col in cursor.description] if cursor.description else []
        rows = cursor.fetchall()

        df = pd.DataFrame.from_records(rows, columns=columns)

        cursor.close()
        return df

    except Exception as e:
        print(f"⚠ Error running query: {e}")
        return pd.DataFrame()
    
def start_new_file(base_output_path, file_index):
    # Logic: Index 1 is the original. Index 2+ adds the suffix.
    if file_index <= 1:
        new_path = f"{base_output_path}.xlsx"
    else:
        new_path = f"{base_output_path}_part{file_index}.xlsx"
    
    print(f"📁 Starting new file: {os.path.basename(new_path)}")
    return new_path
def process_system_refreshes(file_path, metric_col='METRIC_ID', time_col='ACCESS_TIME'):
    """
    Called after the data pull is finished. 
    Reads the output file, identifies refreshes, and saves multi-sheet Excel.
    """
    if not os.path.exists(file_path):
        return

    print(f"--- Post-Processing System Refreshes for {os.path.basename(file_path)} ---")
    
    # Load the data we just wrote
    df = pd.read_excel(file_path)
    df.columns = df.columns.str.strip()
    df[time_col] = pd.to_datetime(df[time_col])

    # Reuse your existing logic function
    # (Assuming the logic from our previous conversation is named 'mark_even_minute_intervals')
    processed_df, summary_df = rt.mark_even_minute_intervals(df, metric_col, time_col)

    # Overwrite the file with the two-sheet version
    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        processed_df.to_excel(writer, sheet_name='Processed_Logs', index=False)
        summary_df.to_excel(writer, sheet_name='System_Summary', index=False)
        
        # Formatting
        #workbook = writer.book
        for sheet_name in writer.sheets:
            writer.sheets[sheet_name].set_column('A:Z', 18)
            
    print(f"✅ Post-processing complete. Sheets created in {os.path.basename(file_path)}")

# ----------------------------
# MAIN
# ----------------------------
def main():
    root = Tk()
    root.withdraw()
    ILLEGAL_CHARS_RE = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')
    files_to_process = []
    # --- Dates ---
    archive_end, live_start = get_archive_cutoff()
  
    start_date_str = simpledialog.askstring("Start Date", "Enter start date (Any format):")
    end_date_str = simpledialog.askstring("End Date", "Enter end date (Optional for single date):")
    try:
        # Use pandas to flexibily parse the start date
        # dayfirst=True is helpful if you are outside the US!
        start_date = pd.to_datetime(start_date_str).to_pydatetime()

        # Handle the end date logic
        if not end_date_str or end_date_str.strip() == "":
            end_date = start_date
        else:
            end_date = pd.to_datetime(end_date_str).to_pydatetime()

        # Normalize to start and end of day
        start_date = datetime.combine(start_date.date(), dt_time.min)
        end_date = datetime.combine(end_date.date(), dt_time.max)

    except Exception as e:
        print(f"Could not understand the date format: {e}")

    tables=resolve_tables(start_date, live_start)

    # --- Output folder ---
    output_folder = filedialog.askdirectory(title="Select output folder")
    if not output_folder:
        return
    output_path = os.path.join(output_folder, "query_output.xlsx")

    # --- Pick query ---
    available_queries = list(queries.keys())
    query_choice = pick_query_type(available_queries)
    base_query = queries[query_choice]

    # --- Filter type ---
    filter_type = simpledialog.askstring(
        "Filter Type",
        "Enter filter type: p-patient_only, u-user_only, b-both:"
    ).strip().lower()
    if filter_type not in ("p", "u", "b"):
        raise ValueError(f"Invalid option: {filter_type}")
    

    patient_id = None
    PatientName = None
    user_login = None
    if filter_type in ("p", "b"):
        patient_id = simpledialog.askstring("Patient ID", "Enter Patient ID:")
        PatientName = simpledialog.askstring("Patient Name", "Enter Patient Name (Optional):")
    if filter_type in ("u", "b"):
        user_login = simpledialog.askstring("User Login", "Enter User Login:")

    # --- Open single connection ---
    conn = get_connection(tables["server"], tables["database"])
    user_id = get_user_id(conn, user_login) if user_login else None
    
    if filter_type in ("u", "b") and not user_id:
        messagebox.showerror("Error", f"User login '{user_login}' not found.")
        conn.close()
        return

    # --- Prepare output file ---
    prepared_ts = datetime.now().strftime("%Y%m%d_%H%M")
    safe_start_str = start_date.strftime("%Y%m%d")
    safe_end_str = end_date.strftime("%Y%m%d")
    PatientName = PatientName  or "_"
    user_login = user_login or "_AllUsers"
    filename_str = f"{query_choice}{PatientName}_{user_login}_{safe_start_str}_to_{safe_end_str}_prepared_{prepared_ts}"
    filename_str=re.sub(r'_{2,}', '_',filename_str)
    base_output_path = os.path.join(output_folder, filename_str)

    first_write = True
    crossed_boundary = start_date >= archive_end
    #Only used if deduping is implemented
    # archive_keys=set()
    current_conn = None
    current_db_key = None   # Used to detect when DB changes
    total_rows_written = 0
    MAX_ROWS_PER_FILE = 750_000
    file_index = 1
    output_path = start_new_file(base_output_path, file_index) # Sets it to 'filename.xlsx'
    if output_path not in files_to_process:
        files_to_process.append(output_path)
    rows_in_current_file = 0
    files_created = 1
    largest_file_rows = 0
    chunk_times = []
    
    live_start_csv_row = None
    start_total = stopwatch.time()

    # --- Loop through date chunks ---
    for i, (chunk_start, chunk_end) in enumerate(get_date_ranges(start_date, end_date, archive_end)):
        start_chunk = stopwatch.time()
        # --- Resolve correct tables for THIS chunk ---
        tables = resolve_tables(chunk_start, live_start)

        db_key = (tables["server"], tables["database"])

        # --- Reconnect ONLY if DB changes ---
        if db_key != current_db_key:
            if current_conn:
                current_conn.close()

            print(f"Connecting to {tables['server']} / {tables['database']}")
            current_conn = get_connection(tables["server"], tables["database"])
            current_db_key = db_key

        # --- Determine archive vs live ---
        is_live = chunk_start >= live_start

        where_clause = build_where_clause(filter_type, patient_id, user_login) or ""

        query_text = base_query.format(
            access_log=tables["access_log"],
            acc_log_dtl=tables["acc_log_dtl"],
            acc_log_MTDTL=tables["acc_log_MTDTL"],
            acc_WRKF=tables["acc_WRKF"],
            start_date=chunk_start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            end_date=chunk_end.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            patient_id=patient_id or "",
            user_login=user_login or "",
            user_id=user_id or "NULL",
            where_clause=where_clause
        )

        print(f"Running chunk {i+1}: {chunk_start} → {chunk_end} ({tables['source']})")

        df = run_query_pyodbc_conn(current_conn, query_text)

        if df.empty:
            print("   No records for this chunk.")
            continue

        #for col in df.select_dtypes(include="object").columns:
        #    df[col] = df[col].apply(
        #        lambda x: ILLEGAL_CHARS_RE.sub('', x) if isinstance(x, str) else x
        #    )

        string_cols = df.select_dtypes(include=["object", "string"]).columns
        df[string_cols] = df[string_cols].replace(ILLEGAL_CHARS_RE, "", regex=True)  
        df['server_source'] = tables['server']  
        # --- Dedupe if crossing from archive to live ---
        # Add keys here as a possibility.
        # Detect first live chunk
        if is_live and not crossed_boundary:
            crossed_boundary = True
            live_start_csv_row = total_rows_written + 2
            print(f"⚠ Archive → Live boundary crossed at CSV row {live_start_csv_row}")

        rows_written_this_chunk = len(df)

        # --- Check if we need a new file ---
        if rows_in_current_file + rows_written_this_chunk > MAX_ROWS_PER_FILE:
            largest_file_rows = max(largest_file_rows, rows_in_current_file)
            file_index += 1
            files_created += 1
            rows_in_current_file = 0
            first_write = True
            output_path = start_new_file(base_output_path, file_index)

        # --- Write to Excel ---
        if first_write:
            df.to_excel(output_path, index=False, header=True)
            first_write = False
        else:
            with pd.ExcelWriter(output_path, mode="a", engine="openpyxl", if_sheet_exists="overlay") as writer:
                start_row = writer.sheets['Sheet1'].max_row
                df.to_excel(writer, index=False, header=False, startrow=start_row)

        rows_in_current_file += rows_written_this_chunk
        total_rows_written += rows_written_this_chunk
        print(f"Chunk {i+1} written: {rows_written_this_chunk} rows → {os.path.basename(output_path)}")
        chunk_runtime = stopwatch.time() - start_chunk
        chunk_times.append(chunk_runtime)

        elapsed_chunk = timedelta(seconds=int(chunk_runtime))
        elapsed_total = timedelta(seconds=int(stopwatch.time() - start_total))
        print(f"Elapsed time {elapsed_chunk}")
        print(f"Total Time {elapsed_total}\n")
        largest_file_rows = max(largest_file_rows, rows_in_current_file)

    if current_conn:
        current_conn.close()

    if total_rows_written == 0:
        print("⚠️ No rows returned across ALL chunks.")
        print("No output file was created.")

    if total_rows_written > 0:
        # Determine if we should prompt or just run
        # Default 'Yes' for cyber_patientless, otherwise prompt the user
        avg_chunk_time = sum(chunk_times) / len(chunk_times) if chunk_times else 0
        avg_chunk_time_td = timedelta(seconds=int(avg_chunk_time))

        print(f"✅ Total rows written across all chunks: {total_rows_written}")
        print(f"📁 Files created: {files_created}")
        print(f"📊 Largest file rows: {largest_file_rows:,}")
        print(f"⏱ Average chunk time: {avg_chunk_time_td}")
        print(f"⏱ Total runtime: {timedelta(seconds=int(stopwatch.time() - start_total))}")
        print(f"\nOutput saved starting at:\n{base_output_path}.xlsx")
        should_process = False
        
        if query_choice == "cyber_patientless":
            should_process = True
            print("Query type 'cyber_patientless' detected: Auto-processing System Refreshes...")
        else:
            should_process = messagebox.askyesno(
                "Process Refreshes?", 
                f"Data pull complete ({total_rows_written} rows).\n\n"
                "Would you like to run the System Refresh Analysis on the output file(s)?"
            )

        if should_process:
            for f_path in files_to_process:
                try:
                    process_system_refreshes(f_path, metric_col='METRIC_ID', time_col='ACCESS_TIME')
                except Exception as e:
                    print(f"❌ Error processing refreshes for {f_path}: {e}")
        else:
            print("⏩ System Refresh Analysis skipped by user.")
    finished_sound()    

if __name__ == "__main__":
    main()
