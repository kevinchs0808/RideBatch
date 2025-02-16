import subprocess
import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook

# Get Airflow MySQL Connection
def get_mysql_connection():
    conn = BaseHook.get_connection("my_rds")
    return f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"

# Function to Upload CSV to MySQL RDS
def upload_csv_to_rds():
    engine = create_engine(get_mysql_connection())

    # Read CSV file
    df = pd.read_csv('/path/to/taxi+_zone_lookup.csv')  # Update path if necessary

    # Rename columns if needed (ensure they match the MySQL schema)
    df.columns = ['LocationID', 'Borough', 'Zone', 'service_zone']

    # Insert into MySQL (replace table name if different)
    df.to_sql('taxi_zone_lookup', engine, if_exists='replace', index=False)

    print("✅ CSV uploaded to RDS successfully!")

def run_spark_transform():
    subprocess.run([
        "spark-submit",
        "--deploy-mode", "cluster",  # Can also be 'client' depending on your setup
        "--master", "yarn",  # Assuming EMR is using YARN
        "s3://your-bucket/path/to/spark_transform.py"
    ], check=True)

def generate_dashboard():
    subprocess.run(["python3", "generate_dashboard.py"], check=True)

def send_email():
    subprocess.run(["python3", "send_email_report.py"], check=True)

def clean_up():
    subprocess.run(["rm", "-f", "nyc_taxi_report.pdf"], check=True)
