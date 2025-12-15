"""
Kafka Stream Orchestration DAG
Demonstrates orchestrating Kafka producers/consumers as part of data pipeline
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kafka_stream_orchestration',
    default_args=default_args,
    description='Orchestrate Kafka streaming pipeline',
    schedule_interval='@hourly',
    catchup=False,
    tags=['kafka', 'streaming', 'real-time'],
)


def check_kafka_health(**context):
    """Check if Kafka cluster is healthy"""
    from kafka import KafkaAdminClient
    from kafka.errors import KafkaError

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers='kafka:9092',
            client_id='airflow-health-check'
        )
        # List topics to verify connection
        topics = admin_client.list_topics()
        print(f"Kafka is healthy. Topics: {topics}")
        admin_client.close()
        return True
    except KafkaError as e:
        print(f"Kafka health check failed: {e}")
        return False


def start_producer(**context):
    """Start Kafka producer to generate events"""
    print("Starting Kafka producer...")
    # In production, this would trigger producer script
    # For demo, we'll use BashOperator to run the producer script


def monitor_consumer(**context):
    """Monitor consumer lag and processing"""
    print("Monitoring consumer lag...")
    # In production, check consumer group lag
    # Example: kafka-consumer-groups --describe --group my-group


def validate_data_flow(**context):
    """Validate that data is flowing through Kafka"""
    from kafka import KafkaConsumer
    import json

    try:
        consumer = KafkaConsumer(
            'user-events',
            bootstrap_servers='kafka:9092',
            auto_offset_reset='latest',
            enable_auto_commit=False,
            consumer_timeout_ms=10000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        message_count = 0
        for message in consumer:
            message_count += 1
            if message_count >= 10:  # Check first 10 messages
                break

        consumer.close()

        print(f"Validated {message_count} messages")
        context['ti'].xcom_push(key='message_count', value=message_count)

        return message_count > 0
    except Exception as e:
        print(f"Validation failed: {e}")
        return False


# Define tasks
health_check = PythonOperator(
    task_id='check_kafka_health',
    python_callable=check_kafka_health,
    dag=dag,
)

run_producer = BashOperator(
    task_id='run_producer',
    bash_command='python /opt/airflow/kafka/producer/event_producer.py --duration 300 &',
    dag=dag,
)

run_consumer = BashOperator(
    task_id='run_consumer',
    bash_command='python /opt/airflow/kafka/consumer/event_consumer.py --duration 300 &',
    dag=dag,
)

validate_flow = PythonOperator(
    task_id='validate_data_flow',
    python_callable=validate_data_flow,
    dag=dag,
)

monitor = PythonOperator(
    task_id='monitor_consumer',
    python_callable=monitor_consumer,
    dag=dag,
)

# Dependencies
health_check >> run_producer >> run_consumer >> validate_flow >> monitor
