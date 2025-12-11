"""
Data Quality Pipeline DAG
Demonstrates data quality checks, branching logic, and monitoring
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
import pandas as pd

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_quality_pipeline',
    default_args=default_args,
    description='Data quality checks with branching logic',
    schedule_interval='@hourly',
    catchup=False,
    tags=['data-quality', 'monitoring', 'validation'],
)


def check_data_freshness(**context):
    """Check if data is fresh (updated in last hour)"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()

    query = """
        SELECT MAX(created_at) as last_update
        FROM raw.user_events
    """

    df = pd.read_sql(query, conn)
    conn.close()

    last_update = pd.to_datetime(df['last_update'].iloc[0])
    now = pd.Timestamp.now()
    minutes_old = (now - last_update).total_seconds() / 60

    print(f"Data is {minutes_old:.1f} minutes old")

    # Store result in XCom
    context['ti'].xcom_push(key='data_age_minutes', value=minutes_old)

    return minutes_old < 60  # Fresh if less than 60 minutes old


def check_data_completeness(**context):
    """Check for null values and completeness"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()

    query = """
        SELECT
            COUNT(*) as total_records,
            SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) as null_user_id,
            SUM(CASE WHEN event_type IS NULL THEN 1 ELSE 0 END) as null_event_type,
            SUM(CASE WHEN event_timestamp IS NULL THEN 1 ELSE 0 END) as null_timestamp
        FROM raw.user_events
        WHERE created_at >= NOW() - INTERVAL '1 hour'
    """

    df = pd.read_sql(query, conn)
    conn.close()

    total = df['total_records'].iloc[0]
    null_count = df['null_user_id'].iloc[0] + df['null_event_type'].iloc[0] + df['null_timestamp'].iloc[0]
    completeness = (1 - (null_count / (total * 3))) * 100 if total > 0 else 0

    print(f"Data completeness: {completeness:.2f}%")

    context['ti'].xcom_push(key='completeness_score', value=completeness)

    return completeness >= 95  # Pass if >= 95% complete


def check_data_volume(**context):
    """Check if data volume is within expected range"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()

    query = """
        SELECT COUNT(*) as record_count
        FROM raw.user_events
        WHERE created_at >= NOW() - INTERVAL '1 hour'
    """

    df = pd.read_sql(query, conn)
    conn.close()

    record_count = df['record_count'].iloc[0]

    print(f"Record count in last hour: {record_count}")

    context['ti'].xcom_push(key='record_count', value=record_count)

    # Expected: at least 100 records per hour
    return record_count >= 100


def quality_check_router(**context):
    """Route based on quality check results"""
    freshness = context['ti'].xcom_pull(task_ids='check_freshness')
    completeness = context['ti'].xcom_pull(task_ids='check_completeness')
    volume = context['ti'].xcom_pull(task_ids='check_volume')

    if freshness and completeness and volume:
        return 'quality_passed'
    else:
        return 'quality_failed'


def send_alert(**context):
    """Send alert when quality checks fail"""
    data_age = context['ti'].xcom_pull(key='data_age_minutes', task_ids='check_freshness')
    completeness = context['ti'].xcom_pull(key='completeness_score', task_ids='check_completeness')
    record_count = context['ti'].xcom_pull(key='record_count', task_ids='check_volume')

    alert_message = f"""
    Data Quality Alert!
    - Data Age: {data_age:.1f} minutes
    - Completeness: {completeness:.2f}%
    - Record Count: {record_count}

    Please investigate immediately.
    """

    print(alert_message)
    # In production, send to Slack/PagerDuty/Email


def log_metrics(**context):
    """Log quality metrics for monitoring"""
    data_age = context['ti'].xcom_pull(key='data_age_minutes', task_ids='check_freshness')
    completeness = context['ti'].xcom_pull(key='completeness_score', task_ids='check_completeness')
    record_count = context['ti'].xcom_pull(key='record_count', task_ids='check_volume')

    metrics = {
        'timestamp': datetime.now().isoformat(),
        'data_age_minutes': data_age,
        'completeness_percentage': completeness,
        'record_count': record_count
    }

    print(f"Quality Metrics: {metrics}")
    # In production, send to monitoring system (Prometheus, Datadog, etc.)


# Define tasks
start = EmptyOperator(task_id='start', dag=dag)

freshness_check = PythonOperator(
    task_id='check_freshness',
    python_callable=check_data_freshness,
    dag=dag,
)

completeness_check = PythonOperator(
    task_id='check_completeness',
    python_callable=check_data_completeness,
    dag=dag,
)

volume_check = PythonOperator(
    task_id='check_volume',
    python_callable=check_data_volume,
    dag=dag,
)

route_task = BranchPythonOperator(
    task_id='quality_check_router',
    python_callable=quality_check_router,
    dag=dag,
)

quality_passed = EmptyOperator(task_id='quality_passed', dag=dag)

quality_failed = PythonOperator(
    task_id='quality_failed',
    python_callable=send_alert,
    dag=dag,
)

log_task = PythonOperator(
    task_id='log_metrics',
    python_callable=log_metrics,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success', dag=dag)

# Define task dependencies
start >> [freshness_check, completeness_check, volume_check] >> route_task
route_task >> [quality_passed, quality_failed] >> log_task >> end
