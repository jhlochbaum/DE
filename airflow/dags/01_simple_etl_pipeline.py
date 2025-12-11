"""
Simple ETL Pipeline DAG
Demonstrates basic Extract, Transform, Load pattern with PostgreSQL
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_etl_pipeline',
    default_args=default_args,
    description='Basic ETL pipeline demonstrating data extraction, transformation, and loading',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'postgres', 'example'],
)


def extract_data(**context):
    """Extract data from source table"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()

    query = """
        SELECT
            transaction_id,
            user_id,
            product_id,
            amount,
            transaction_date,
            status
        FROM raw.sales_transactions
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '1 day'
        AND status = 'completed'
    """

    df = pd.read_sql(query, conn)
    conn.close()

    # Save to XCom for next task
    context['ti'].xcom_push(key='raw_data', value=df.to_json(orient='records'))
    print(f"Extracted {len(df)} records")


def transform_data(**context):
    """Transform extracted data"""
    # Get data from previous task
    raw_data = context['ti'].xcom_pull(key='raw_data', task_ids='extract')
    df = pd.read_json(raw_data)

    # Apply transformations
    df['amount_usd'] = df['amount']  # Already in USD
    df['transaction_year'] = pd.to_datetime(df['transaction_date']).dt.year
    df['transaction_month'] = pd.to_datetime(df['transaction_date']).dt.month
    df['transaction_day'] = pd.to_datetime(df['transaction_date']).dt.day

    # Aggregate by user
    user_summary = df.groupby('user_id').agg({
        'transaction_id': 'count',
        'amount': ['sum', 'mean', 'max']
    }).reset_index()

    user_summary.columns = ['user_id', 'transaction_count', 'total_amount', 'avg_amount', 'max_amount']

    # Save to XCom
    context['ti'].xcom_push(key='transformed_data', value=user_summary.to_json(orient='records'))
    print(f"Transformed data for {len(user_summary)} users")


def load_data(**context):
    """Load transformed data into target table"""
    transformed_data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform')
    df = pd.read_json(transformed_data)

    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()

    # Load to staging table
    df.to_sql(
        'daily_user_sales_summary',
        engine,
        schema='staging',
        if_exists='append',
        index=False
    )

    print(f"Loaded {len(df)} records to staging.daily_user_sales_summary")


# Define tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
)

# Create staging table if not exists
create_staging_table = PostgresOperator(
    task_id='create_staging_table',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS staging.daily_user_sales_summary (
            user_id INTEGER,
            transaction_count INTEGER,
            total_amount DECIMAL(10, 2),
            avg_amount DECIMAL(10, 2),
            max_amount DECIMAL(10, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    dag=dag,
)

# Define task dependencies
create_staging_table >> extract_task >> transform_task >> load_task
