"""
Spark Data Processing DAG
Demonstrates integration with Apache Spark for large-scale data processing
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_data_processing',
    default_args=default_args,
    description='Process large datasets using Apache Spark',
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'big-data', 'etl'],
)


def prepare_spark_job(**context):
    """Prepare parameters for Spark job"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')

    params = {
        'input_path': f'/opt/spark-data/raw/',
        'output_path': f'/opt/spark-data/processed/{execution_date}',
        'execution_date': execution_date
    }

    context['ti'].xcom_push(key='spark_params', value=params)
    print(f"Prepared Spark job for {execution_date}")


prepare_job = PythonOperator(
    task_id='prepare_spark_job',
    python_callable=prepare_spark_job,
    dag=dag,
)

# Submit Spark job
spark_job = SparkSubmitOperator(
    task_id='run_spark_processing',
    application='/opt/spark-jobs/data_aggregation.py',
    conn_id='spark_default',
    conf={
        'spark.executor.memory': '2g',
        'spark.driver.memory': '1g',
        'spark.executor.cores': '2',
    },
    application_args=[
        '--input', '/opt/spark-data/raw',
        '--output', '/opt/spark-data/processed',
    ],
    dag=dag,
)

# Validate output
validate_output = BashOperator(
    task_id='validate_output',
    bash_command='ls -lh /opt/spark-data/processed/ && echo "Spark job completed successfully"',
    dag=dag,
)

prepare_job >> spark_job >> validate_output
