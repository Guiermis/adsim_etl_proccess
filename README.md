# 📊 CRM Data Pipeline for Power BI Dashboard


Automated ETL pipeline to sync CRM data (deals, clients, sales) with a PostgreSQL database, powering real-time analytics in Power BI.
## 🚀 Features

    Daily automated sync of CRM data (deals, organizations, sales) via AdSim API.

    Smart diff-based updates to minimize database writes (find_differences() function).

    Client classification (e.g., "New", "Recurring", "Seasonal") using temporal logic.

    Error resilience: Robust logging, JSON error reports, and NaN/NULL handling.

    Multi-source integration: Pulls data from APIs, Google Sheets, and Excel.

## ⚙️ Tech Stack

    Category	Tools
    
    Languages	Python 3.9+
    
    Libraries	pandas, psycopg2, SQLAlchemy, gspread, requests
    
    Database	PostgreSQL

    APIs	        AdSim CRM API (REST/NDJSON)

    Scheduling	Cron (Linux) / Task Scheduler (Windows)
    
## 📂 Project Structure

    adsim_sql/
      ├── adsim_sql.py            # Main ETL script
      ├── adsim_config.py         # Config (API tokens, DB credentials)
      ├── json_files/             # Google Sheets API credentials
      │   └── credentials.json    
      ├── xlsx_files/             # Reference Excel files
      │   ├── matriz_equipes.xlsx
      │   └── IDS_TargetsDigital.xlsx
      ├── reports/                # Auto-generated error logs
      │   └── script_report_*.json
      └── README.md

## 🛠️ Setup & Usage
  ### Prerequisites

    PostgreSQL database (schema matches expected_columns in script).

    AdSim CRM API token (set in adsim_config.py).

    Google Service Account JSON (for Sheets integration).

## Installation

    git clone https://github.com/yourusername/crm-data-pipeline.git
    cd crm-data-pipeline
    pip install -r requirements.txt  # Install dependencies

## Configuration

    Rename adsim_config.example.py to adsim_config.py and fill in:
    python
    Copy

    adsim_token = "YOUR_ADSIM_API_TOKEN"
    host = "your_postgres_host"
    dbname = "your_database_name"
    user = "your_db_user"
    password = "your_db_password"

    Place Google Sheets credentials in json_files/credentials.json.

## Run Manually
  
    python adsim_sql.py

## Schedule Daily Runs

  ### Linux (Cron):

    0 8 * * * /usr/bin/python3 /path/to/adsim_sql.py >> /path/to/logs/crm_pipeline.log 2>&1

  ### Windows (Task Scheduler):

    Trigger: Daily at 8 AM

    Action: python.exe C:\path\to\adsim_sql.py

## 🔍 Key Functions

 ### Function	Purpose
      
      find_differences()	Compares API data with DB to identify updates/inserts.

      update_or_insert_rows()	Executes batched SQL updates/inserts with error handling.

      classify_deal()	Tags deals as "New/Recurring" based on 18-month activity.

      safe_merge()	Handles merges between inconsistent datasets (e.g., executive name maps).

## 🤝 Contributing

    Fork the repository.

    Create a branch (git checkout -b feature/your-feature).

    Commit changes (git commit -m 'Add feature').

    Push to the branch (git push origin feature/your-feature).

    Open a Pull Request.

## 📜 License
MIT

  ### Why This Project?

    This pipeline replaced a manual 4-hour daily process with an automated solution, enabling:

    Real-time analytics for sales teams.

    Accurate forecasting via client segmentation.

    Data consistency across CRM, finance, and BI systems.
