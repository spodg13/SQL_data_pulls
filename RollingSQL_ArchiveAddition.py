import os
import pyodbc
import openpyxl
import pandas as pd
from datetime import datetime, timedelta
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
        r"DRIVER={SQL Server};"
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
    cutoff = today - timedelta(days=(3 * 365 ))
    return cutoff.replace(hour=0, minute=0, second=0, microsecond=0)
    

def resolve_tables(chunk_start,archive_cutoff):
    
    if chunk_start < archive_cutoff:
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
            "access_log": f"clarity_rpt.dbo.ACCESS_LOG",
            "acc_log_dtl": f"clarity_rpt.dbo.ACC_LOG_DTL_IX",
            "acc_log_MTDTL": f"clarity_rpt.dbo.ACC_LOG_MTLDTL_IX",
            "acc_WRKF": f"clarity_rpt.dbo.ACCESS_WRKF"
        }
# ----------------------------
# CHUNKED DATE RANGE
# ----------------------------
def get_date_ranges(start_date, end_date):
    current = start_date

    while current <= end_date:
        chunk_end = current + timedelta(days=CHUNK_SIZE_MONTHS * 30)
        
        # Hard stop at end of the var-current year
        year_end = datetime(
            year=current.year,
            month=12,
            day=31,
            hour=23,
            minute=59,
            second=59,
            microsecond=999000
        )
        chunk_end = min(chunk_end, year_end, end_date)

        yield current, chunk_end

        # Move one millisecond past chunk_end to avoid infinite loop
        current = chunk_end + timedelta(milliseconds=1)


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
# ----------------------------
# MAIN
# ----------------------------
def main():
    root = Tk()
    root.withdraw()
    ILLEGAL_CHARS_RE = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')

    # --- Dates ---
    archive_cutoff = get_archive_cutoff()
    start_date_str = simpledialog.askstring("Start Date", "Enter start date (YYYY-MM-DD):")
    end_date_str = simpledialog.askstring("End Date", "Enter end date (YYYY-MM-DD):")
    if not end_date_str or end_date_str.strip() == "":
        end_date_str = start_date_str
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Add 00:00:00.000 and 23:59:59.999
    start_date = datetime.combine(start_date, datetime.min.time())
    end_date = datetime.combine(end_date, datetime.max.time())
    tables=resolve_tables(start_date, archive_cutoff)

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
    user_login = None
    if filter_type in ("p", "b"):
        patient_id = simpledialog.askstring("Patient ID", "Enter Patient ID:")
    if filter_type in ("u", "b"):
        user_login = simpledialog.askstring("User Login", "Enter User Login:")

    # --- Open single connection ---
    conn = get_connection(tables["server"], tables["database"])
    user_id = get_user_id(conn, user_login) if user_login else None
    PatientName = None

    if filter_type in ("u", "b") and not user_id:
        messagebox.showerror("Error", f"User login '{user_login}' not found.")
        conn.close()
        return

    # --- Prepare output file ---
    prepared_ts = datetime.now().strftime("%Y%m%d_%H%M")
    PatientName = PatientName  or ""
    user_login = user_login or ""
    output_path = os.path.join(output_folder, f"{query_choice}_{PatientName}_{user_login}_{start_date_str}_to_{end_date_str}_prepared_{prepared_ts}.xlsx")

    first_write = True
    crossed_boundary = start_date >= archive_cutoff
    #Only used if deduping is implemented
    # archive_keys=set()
    current_conn = None
    current_db_key = None   # Used to detect when DB changes
    total_rows_written = 0
    live_start_csv_row = None
    
    # --- Loop through date chunks ---
    for i, (chunk_start, chunk_end) in enumerate(get_date_ranges(start_date, end_date)):

        # --- Resolve correct tables for THIS chunk ---
        tables = resolve_tables(chunk_start, archive_cutoff)

        db_key = (tables["server"], tables["database"])

        # --- Reconnect ONLY if DB changes ---
        if db_key != current_db_key:
            if current_conn:
                current_conn.close()

            print(f"Connecting to {tables['server']} / {tables['database']}")
            current_conn = get_connection(tables["server"], tables["database"])
            current_db_key = db_key

        # --- Determine archive vs live ---
        is_live = chunk_start >= archive_cutoff

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

        print(f"Running chunk {i+1}: {chunk_start} → {chunk_end}")

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
        # --- Dedupe if crossing from archive to live ---
        # Add keys here as a possibility.
        # Detect first live chunk
        if is_live and not crossed_boundary:
            crossed_boundary = True
            live_start_csv_row = total_rows_written + 2
            print(f"⚠ Archive → Live boundary crossed at CSV row {live_start_csv_row}")

        # Write to Excel
        if first_write:
            df.to_excel(output_path, index=False, header=True)
            first_write = False
        else:
            with pd.ExcelWriter(output_path, mode="a", engine="openpyxl", if_sheet_exists="overlay") as writer:
                start_row = writer.sheets['Sheet1'].max_row
                df.to_excel(writer, index=False, header=False, startrow=start_row)
        rows_written_this_chunk = len(df)        
        total_rows_written += rows_written_this_chunk
        print(f"Chunk {i+1} written: {rows_written_this_chunk} rows")

    if current_conn:
        current_conn.close()
    
    if total_rows_written == 0:
        print("⚠️ No rows returned across ALL chunks.")
        print("No output file was created.")
    else:
        print(f"✅ Total rows written across all chunks: {total_rows_written}")
        print(f"All queries finished.\nOutput saved to:\n{output_path}")

if __name__ == "__main__":
    main()
