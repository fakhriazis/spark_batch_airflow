# ASSIGNMENT DAY 22
# FAKHRI AZIS BASIRI
# DATA ENGINEER BATCH 7

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine

# Fungsi untuk membaca data hasil Spark dan melakukan analisis di PostgreSQL
def analyze_data():
    # Membuat koneksi ke database PostgreSQL
    engine = create_engine("postgresql+psycopg2://user:password@localhost:5432/warehouse")
    connection = engine.connect()

    # Membaca data hasil ETL dari tabel PostgreSQL
    df_transformed = pd.read_sql("SELECT * FROM transformed_data", connection)
    df_sales_by_country = pd.read_sql("SELECT * FROM sales_by_country", connection)
    df_sales_by_month = pd.read_sql("SELECT * FROM sales_by_month", connection)

    # Analisis agregasi sederhana
    total_sales = df_transformed['TotalPrice'].sum()
    print(f"Total Sales: {total_sales}")

    # Analisis churn-retention sederhana
    customer_churn = df_transformed.groupby('CustomerID').size().value_counts()
    print("Customer Churn/Retention Analysis:")
    print(customer_churn)

    # Simpan hasil analisis ke CSV dan ke PostgreSQL
    df_sales_by_country.to_csv('/data/sales_by_country.csv', index=False)
    df_sales_by_month.to_csv('/data/sales_by_month.csv', index=False)
    df_sales_by_country.to_sql('analysis_sales_by_country', engine, if_exists='replace', index=False)
    df_sales_by_month.to_sql('analysis_sales_by_month', engine, if_exists='replace', index=False)

    # Tutup koneksi
    connection.close()

# Definisi DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spark_etl_analysis_dag',
    default_args=default_args,
    description='ETL and Analysis DAG with Spark and PostgreSQL',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Submit Spark job untuk melakukan ETL
    etl_job = SparkSubmitOperator(
    task_id='submit_spark_job',
    application='/spark-scripts/spark_etl_job.py',  # Path ke file Spark
    conn_id='spark_main',
    application_args=[
        '--pg-url', 'jdbc:postgresql://localhost:5432/warehouse',  # URL PostgreSQL
        '--pg-user', 'user',
        '--pg-password', 'password',  # Masukkan password dengan benar
    ],
    name='spark_etl_job',
    execution_timeout=timedelta(minutes=1),
)


    # Task 2: Membaca data dari PostgreSQL dan melakukan analisis
    analyze_task = PythonOperator(
        task_id='analyze_data',
        python_callable=analyze_data,
    )

    # Definisikan urutan eksekusi task
    etl_job >> analyze_task
