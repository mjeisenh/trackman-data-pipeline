## === csv_to_db_uploader === ##
# Processes CSVs downloaded by the FTP script (dated by YYYY/MM/DD)
# Cleans, validates, and uploads cleaned data to Azure SQL Server
# Logs each action and reports batch-wise performance

import re 
import pandas as pd
import logging
import os
import pyodbc
import time
import psutil
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Setup Logging
log_dir = os.getenv("LOCAL_BASE_DIR", ".") + "/Logs"
os.makedirs(log_dir, exist_ok=True)
current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
log_path = os.path.join(log_dir, f'local_to_db_log_{current_time}.log')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_path), logging.StreamHandler()])

def get_yesterday_dir():
    # Match the FTP script's directory structure (e.g., /v3/YYYY/MM/DD)
    yesterday = datetime.now() - timedelta(days=1)
    return f"/v3/{yesterday.year}/{yesterday.strftime('%m')}/{yesterday.strftime('%d')}"

def list_csv_files(root_dir):
    # Get list of CSV files for upload, skipping unverified or player tracking data
    csv_files = []
    unverified = 0
    player_pos = 0
    total = 0

    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if 'unverified' in filename.lower():
                unverified += 1
                continue
            if 'playerpositioning' in filename.lower() or 'playertracking' in filename.lower():
                player_pos += 1
                continue
            if filename.lower().endswith('.csv'):
                csv_files.append(os.path.join(dirpath, filename))
                total += 1

    logging.info(f"Files skipped: {unverified} unverified, {player_pos} player positioning")
    logging.info(f"Total CSV files to process: {total}")
    return csv_files

def clean_data(df):
    df.rename(columns={'Top/Bottom': 'Top_Bottom'}, inplace=True)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Time'] = pd.to_datetime(df['Time'], format='%H:%M:%S.%f', errors='coerce')
    df['Time'] = df['Time'].fillna(pd.to_datetime(df['Time'], format='%H:%M:%S', errors='coerce'))
    df['Time'] = df['Time'].dt.time
    df['RowID'] = df['GameID'] + '_' + df['PitchNo'].astype(str)
    
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)
    
    return df

def insert_to_db(df, table, engine, file_path):
    try:
        df.to_sql(name=table, con=engine, if_exists='append', index=False)
        logging.info(f"{file_path} → Inserted {len(df)} rows")
    except IntegrityError as e:
        match = re.search(r"The duplicate key value is \((.*?)\)", str(e.orig))
        if match:
            logging.error(f"Integrity error: Duplicate key '{match.group(1)}' in {file_path}")
        else:
            logging.error(f"Integrity error: {e}")
    except SQLAlchemyError as e:
        logging.error(f"SQLAlchemy error for {file_path}: {e}")
    except Exception as e:
        logging.error(f"Unhandled error for {file_path}: {e}")

def main():
    # Load from environment
    db_username = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASS")
    db_server = os.getenv("DB_SERVER")
    db_database = os.getenv("DB_NAME")
    local_base = os.getenv("LOCAL_BASE_DIR")
    table = os.getenv("DB_TABLE")

    yesterday_dir = get_yesterday_dir()
    root_dir = os.path.join(local_base, yesterday_dir.lstrip('/'))

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={db_server};DATABASE={db_database};"
        f"UID={db_username};PWD={db_password}"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}", fast_executemany=True)

    try:
        csv_files = list_csv_files(root_dir)
        if not csv_files:
            logging.info("No CSV files found.")
            return

        batch_size = 100
        total_rows = 0
        start_time = time.time()

        for i in range(0, len(csv_files), batch_size):
            # Optional batch file processing for performance
            batch = csv_files[i:i + batch_size]
            memory_before = psutil.Process().memory_info().rss / 1024 / 1024

            for file_path in batch:
                try:
                    df = pd.read_csv(file_path)
                    # Skipping non D1 data
                    if df['Level'].iloc[0] != 'D1':
                        logging.info(f"Skipped {file_path}, Level = {df['Level'].iloc[0]}")
                        continue
                    df = clean_data(df)
                    insert_to_db(df, table, engine, file_path)
                    total_rows += len(df)
                except Exception as e:
                    logging.error(f"Error reading {file_path}: {e}")

            memory_after = psutil.Process().memory_info().rss / 1024 / 1024
            logging.info(f"Processed batch {i // batch_size + 1} | Mem: {memory_before:.2f}MB → {memory_after:.2f}MB")

        elapsed = time.time() - start_time
        logging.info(f"Complete: {len(csv_files)} files, {total_rows} rows in {elapsed:.2f} sec")

    except Exception as e:
        logging.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()
