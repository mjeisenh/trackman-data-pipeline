## === ftp_csv_downloader === ##
# ETL Pipeline: Trackman FTP Server to Local Directory
# Downloads nightly CSVs from a structured FTP path based on YYYY/MM/DD
# Skips existing files and logs activity to a local text file

from ftplib import FTP_TLS
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# === Config via environment variables ===
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
LOCAL_BASE_DIR = os.getenv("LOCAL_BASE_DIR")

def get_yesterday_remote_dir():
    # Returns FTP server filepath for data from previous day
    yesterday = datetime.now() - timedelta(days=1)
    return f"/v3/{yesterday.year}/{yesterday.strftime('%m')}/{yesterday.strftime('%d')}/CSV"

def connect_ftp_tls():
    # Establishes FTPS connection
    ftps = FTP_TLS()
    ftps.connect(FTP_HOST, 21)
    ftps.login(FTP_USER, FTP_PASS)
    ftps.prot_p()
    return ftps

def list_remote_csvs(ftp, remote_dir):
    # List CSVs in FTP path, skipping files marked unverified
    ftp.cwd(remote_dir)
    return [
        filename for filename in ftp.nlst()
        if filename.lower().endswith('.csv') and 'unverified' not in filename.lower()
    ]

def download_new_files(ftp, remote_dir, remote_files):
    # Create local target directory and download new files
    local_dir_target = os.path.join(LOCAL_BASE_DIR, remote_dir.lstrip('/'))
    os.makedirs(local_dir_target, exist_ok=True)
    local_files = os.listdir(local_dir_target)

    downloaded_count = 0
    for filename in remote_files:
        if filename not in local_files:
            local_path = os.path.join(local_dir_target, filename)
            with open(local_path, "wb") as f:
                print(f"Downloading {filename} to {local_path}...")
                ftp.retrbinary(f"RETR {filename}", f.write)
            print(f"Downloaded: {filename}")
            downloaded_count += 1
        else:
            print(f"Already exists: {filename}")

    return downloaded_count

def write_log(message):
    # Append log to file
    log_dir = os.path.join(LOCAL_BASE_DIR, "Logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "ftp_log.txt")
    with open(log_path, "a") as log:
        log.write(f"{datetime.now()}: {message}\n")

def main():
    remote_dir = get_yesterday_remote_dir()
    print(f"Targeting FTP folder: {remote_dir}")

    ftp = connect_ftp_tls()
    downloaded = 0

    try:
        remote_csvs = list_remote_csvs(ftp, remote_dir)
        if not remote_csvs:
            log_message = "No CSV files found on server."
            print(log_message)
        else:
            downloaded = download_new_files(ftp, remote_dir, remote_csvs)
            if downloaded == 0:
                log_message = "Files already exist. No new downloads."
            else:
                log_message = f"Downloaded {downloaded} new file(s)."
    except Exception as e:
        log_message = f"Error: {e}"
        print(log_message)
    finally:
        ftp.quit()

    write_log(log_message)

if __name__ == "__main__":
    main()
