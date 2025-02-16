from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3DownloadOperator
from airflow.providers.amazon.aws.transfers.s3_to_hive import S3ToHiveOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils import run_spark_transform, upload_csv_to_rds, generate_dashboard, send_email

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'nyc_taxi_monthly_report',
    default_args=default_args,
    description='Automates monthly NYC Taxi report using Airflow, Spark, Hive, and Presto',
    schedule_interval='@monthly',
    catchup=False
)

# Step 1: Download raw data from S3
s3_download = S3DownloadOperator(
    task_id='download_data',
    bucket='nyc-taxi-data-bucket',
    key='raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y-%m") }}.csv',
    dest='/tmp/nyc_taxi.csv',
    aws_conn_id='aws_default',
    dag=dag
)

# Step 2: Preprocess and aggregate data with Spark
spark_task = PythonOperator(
    task_id='run_spark_transform',
    python_callable=run_spark_transform,
    dag=dag
)

# Step 3a: Load processed data into Hive
s3_to_hive = S3ToHiveOperator(
    task_id='load_to_hive',
    s3_bucket='nyc-taxi-processed',
    s3_key='{{ macros.ds_format(ds, "%Y-%m-%d", "%Y-%m") }}/nyc_taxi.parquet',
    hive_table='nyc_taxi_data',
    input_format='PARQUET',
    create_table=True,
    partition={'year': '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}', 'month': '{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}'},
    aws_conn_id='aws_default',
    hive_cli_conn_id='hive_default',
    dag=dag
)

# step 3b: Add Taxi Zones Information into RDS

add_taxi_zones_to_rds = PythonOperator(
    task_id='upload_csv_to_rds',
    python_callable=upload_csv_to_rds,
    dag=dag
)

# Step 4: Generate Dashboard
generate_report = PythonOperator(
    task_id='generate_dashboard',
    python_callable=generate_dashboard,
    dag=dag
)

# Send report to Stakeholders' email
send_report = PythonOperator(
    task_id='send_email_report',
    python_callable=send_email,
    dag=dag
)

# Define DAG dependencies
s3_download >> spark_task
spark_task >> s3_to_hive
spark_task >> add_taxi_zones_to_rds
s3_to_hive >> generate_report
add_taxi_zones_to_rds >> generate_report
generate_report >> send_report
