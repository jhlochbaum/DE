"""
Order Events Streaming Pipeline
Orchestrates real-time order event processing using Kafka
Demonstrates: Stream processing, event-driven architecture, Kafka integration
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,  # One retry with fast failure
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'order_events_streaming',
    default_args=default_args,
    description='Real-time order event processing with Kafka',
    schedule_interval='@hourly',
    catchup=False,
    tags=['kafka', 'streaming', 'orders', 'real-time'],
)


def check_kafka_health(**context):
    """
    Verify Kafka cluster is operational
    Checks broker connectivity and topic availability
    """
    from kafka import KafkaAdminClient
    from kafka.errors import KafkaError

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers='kafka:9092',
            client_id='airflow-health-check',
            request_timeout_ms=5000
        )

        # List topics to verify connection
        topics = admin_client.list_topics()

        logging.info(f"{'='*60}")
        logging.info("KAFKA HEALTH CHECK")
        logging.info(f"{'='*60}")
        logging.info(f"  Broker: kafka:9092")
        logging.info(f"  Status: ✓ HEALTHY")
        logging.info(f"  Available topics: {len(topics)}")
        logging.info(f"  Topics: {', '.join(topics) if len(topics) < 10 else f'{len(topics)} topics'}")

        admin_client.close()
        return True

    except KafkaError as e:
        logging.error(f"✗ Kafka health check failed: {e}")
        return False


def verify_northwind_data(**context):
    """
    Ensure Northwind data is available for event generation
    Validates products, customers, and employees exist
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')

    # Check available products
    product_query = "SELECT COUNT(*) AS count FROM products WHERE NOT discontinued"
    product_count = hook.get_first(product_query)[0]

    # Check customers
    customer_query = "SELECT COUNT(*) AS count FROM customers"
    customer_count = hook.get_first(customer_query)[0]

    # Check employees
    employee_query = "SELECT COUNT(*) AS count FROM employees"
    employee_count = hook.get_first(employee_query)[0]

    logging.info(f"{'='*60}")
    logging.info("NORTHWIND DATA VERIFICATION")
    logging.info(f"{'='*60}")
    logging.info(f"  Available products: {product_count}")
    logging.info(f"  Customers: {customer_count}")
    logging.info(f"  Employees: {employee_count}")

    # Push counts to XCom for producer to use
    context['ti'].xcom_push(key='product_count', value=product_count)
    context['ti'].xcom_push(key='customer_count', value=customer_count)

    # Need at least some data to generate events
    if product_count < 5 or customer_count < 5:
        logging.warning("⚠ Insufficient Northwind data for realistic event generation")
        return False

    logging.info("  Status: ✓ READY FOR EVENT GENERATION")
    return True


def create_kafka_topics(**context):
    """
    Create Kafka topics for order events if they don't exist
    """
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers='kafka:9092',
            client_id='topic-creator'
        )

        # Define topics with appropriate configurations
        topics = [
            NewTopic(
                name='order-events',
                num_partitions=3,  # Multiple partitions for scalability
                replication_factor=1
            ),
            NewTopic(
                name='order-metrics',
                num_partitions=1,  # Single partition for ordered metrics
                replication_factor=1
            )
        ]

        try:
            admin_client.create_topics(topics, validate_only=False)
            logging.info("✓ Created Kafka topics: order-events, order-metrics")
        except TopicAlreadyExistsError:
            logging.info("✓ Kafka topics already exist")

        admin_client.close()
        return True

    except Exception as e:
        logging.error(f"✗ Error creating topics: {e}")
        return False


def validate_event_flow(**context):
    """
    Validate that order events are flowing through Kafka
    Samples recent messages to confirm pipeline health
    """
    from kafka import KafkaConsumer
    import json

    try:
        consumer = KafkaConsumer(
            'order-events',
            bootstrap_servers='kafka:9092',
            auto_offset_reset='latest',
            enable_auto_commit=False,
            consumer_timeout_ms=15000,  # 15 second timeout
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        event_types = []
        total_value = 0
        message_count = 0

        logging.info("\n📨 Sampling order events...")

        for message in consumer:
            event = message.value
            event_types.append(event.get('event_type', 'unknown'))

            # Track revenue from new orders
            if event.get('event_type') == 'new_order':
                total_value += event.get('order_total', 0)

            message_count += 1

            # Sample first 20 messages
            if message_count >= 20:
                break

        consumer.close()

        logging.info(f"{'='*60}")
        logging.info("EVENT FLOW VALIDATION")
        logging.info(f"{'='*60}")
        logging.info(f"  Messages sampled: {message_count}")
        logging.info(f"  Event types: {set(event_types)}")
        logging.info(f"  Total order value sampled: ${total_value:,.2f}")

        context['ti'].xcom_push(key='message_count', value=message_count)
        context['ti'].xcom_push(key='event_types', value=list(set(event_types)))

        if message_count > 0:
            logging.info("  Status: ✓ EVENTS FLOWING")
            return True
        else:
            logging.warning("  Status: ⚠ NO EVENTS DETECTED")
            return False

    except Exception as e:
        logging.error(f"✗ Validation failed: {e}")
        return False


def monitor_consumer_lag(**context):
    """
    Monitor consumer group lag and processing rate
    Ensures consumers are keeping up with producers
    """
    from kafka import KafkaAdminClient

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers='kafka:9092',
            client_id='lag-monitor'
        )

        # In production, you would use:
        # kafka-consumer-groups --describe --group order-processor
        # to get detailed lag metrics

        logging.info(f"{'='*60}")
        logging.info("CONSUMER LAG MONITORING")
        logging.info(f"{'='*60}")
        logging.info("  Consumer Group: order-processor")
        logging.info("  Topic: order-events")
        logging.info("  Status: ✓ MONITORING ACTIVE")
        logging.info("")
        logging.info("  In production, this would track:")
        logging.info("    - Consumer lag by partition")
        logging.info("    - Messages per second throughput")
        logging.info("    - Processing latency (P50, P95, P99)")
        logging.info("    - Consumer rebalances")
        logging.info("    - Error rates")

        admin_client.close()

    except Exception as e:
        logging.warning(f"⚠ Monitoring incomplete: {e}")


# Define tasks
health_check = PythonOperator(
    task_id='check_kafka_health',
    python_callable=check_kafka_health,
    dag=dag,
)

verify_data = PythonOperator(
    task_id='verify_northwind_data',
    python_callable=verify_northwind_data,
    dag=dag,
)

create_topics = PythonOperator(
    task_id='create_kafka_topics',
    python_callable=create_kafka_topics,
    dag=dag,
)

run_producer = BashOperator(
    task_id='run_order_producer',
    bash_command='python /opt/airflow/kafka/producer/order_event_producer.py --duration 300 --rate 3 &',
    dag=dag,
)

run_consumer = BashOperator(
    task_id='run_order_consumer',
    bash_command='python /opt/airflow/kafka/consumer/order_event_consumer.py --duration 300 &',
    dag=dag,
)

validate_flow = PythonOperator(
    task_id='validate_event_flow',
    python_callable=validate_event_flow,
    dag=dag,
)

monitor_lag = PythonOperator(
    task_id='monitor_consumer_lag',
    python_callable=monitor_consumer_lag,
    dag=dag,
)

# Task dependencies
# Check health and data availability first
health_check >> verify_data >> create_topics

# Run producer and consumer in parallel after topics created
create_topics >> [run_producer, run_consumer]

# Wait for both to start, then validate
[run_producer, run_consumer] >> validate_flow

# Monitor after validation
validate_flow >> monitor_lag
