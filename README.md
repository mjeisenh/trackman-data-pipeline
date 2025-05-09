# College Baseball Trackman Data Pipeline

This project automates a data ingestion pipeline for NCAA Division 1 Trackman data, transferring CSVs from a remote FTP server and uploading cleaned data to a SQL database. The pipeline was built specifically to support a D1 Baseball program and their analytics department. Scripts are designed to handle Trackman play by play data and follow a specific structure but can be tweaked to support a general FTP -> SQL DB data pipeline. 

## Features

- Secure FTPS connection to Trackman FTP server
- Dynamic daily folder structure (YYYY/MM/DD)
- Cleans data, converts types, adds unique row ID
- Batch uploads to Azure SQL database using SQLAlchemy
- Logging of script progress and errors

## Scripts

- `ftp_csv_downloader.py` — downloads CSV files from a remote FTP server based on yesterday's date.
- `csv_to_db.py` — processes and uploads cleaned CSV data to an Azure SQL database.

## Requirements
- Python 3.8+
- Azure SQL Server or local SQL Server instance
- ODBC Driver 17 for SQL Server

## Notes
- Both scripts dynamically pull from the previous day's folder.
- Ideal to be run daily via cronjob or other task scheduler
- Logging output is saved with timestamped filenames.

## Automation with Cron
To run this pipeline automatically each day, you can schedule both scripts using `cron` (on macOS or Linux). Below is an example that runs the FTP download at 6:30 AM and database upload at 6:45 AM:

1. From a CLI, open your crontab:
```bash
crontab -e
```

2. Add the following line (adjust as needed):
```bash
30 6 * * * /usr/bin/python3 /your/path/to/scripts/ftp_csv_downloader.py >> /your/path/to/scripts/logs/cron_ftp_log.txt 2>&1
45 6 * * * /usr/bin/python3 /your/path/to/scripts/csv_to_db.py >> /your/path/to/scripts/logs/cron_db_log.txt 2>&1
```

## License
This project is open for educational and portfolio purposes. Modify freely.


