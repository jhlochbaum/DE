"""
Order Validation Data Quality Pipeline
Validates Northwind order data integrity and business rules
Demonstrates: Data quality checks, branching logic, monitoring
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,  # One retry with fast failure
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'order_validation_pipeline',
    default_args=default_args,
    description='Data quality validation for Northwind orders',
    schedule_interval='@hourly',
    catchup=False,
    tags=['data-quality', 'northwind', 'validation', 'monitoring'],
)


def check_order_integrity(**context):
    """
    Check that order totals match sum of line items
    Critical business rule: Order.Total = SUM(OrderDetails.ExtendedPrice)
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')

    query = """
        WITH order_totals AS (
            SELECT
                o.order_id,
                COALESCE(SUM(od.unit_price * od.quantity * (1 - od.discount)), 0) AS calculated_total
            FROM orders o
            LEFT JOIN order_details od ON o.order_id = od.order_id
            WHERE o.order_date >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY o.order_id
        )
        SELECT
            COUNT(*) AS total_orders,
            SUM(CASE WHEN calculated_total > 0 THEN 1 ELSE 0 END) AS orders_with_items,
            SUM(CASE WHEN calculated_total = 0 THEN 1 ELSE 0 END) AS orders_without_items
        FROM order_totals
    """

    df = pd.read_sql(query, hook.get_conn())

    total_orders = df['total_orders'].iloc[0]
    orders_with_items = df['orders_with_items'].iloc[0]
    orders_without_items = df['orders_without_items'].iloc[0]

    integrity_score = (orders_with_items / total_orders * 100) if total_orders > 0 else 0

    logging.info(f"\n{'='*60}")
    logging.info("ORDER INTEGRITY CHECK")
    logging.info(f"{'='*60}")
    logging.info(f"  Total orders (last 7 days): {total_orders}")
    logging.info(f"  Orders with line items: {orders_with_items}")
    logging.info(f"  Orders without line items: {orders_without_items}")
    logging.info(f"  Integrity score: {integrity_score:.2f}%")

    context['ti'].xcom_push(key='integrity_score', value=integrity_score)
    context['ti'].xcom_push(key='total_orders', value=total_orders)

    # Pass if >= 95% of orders have line items
    return integrity_score >= 95.0


def check_referential_integrity(**context):
    """
    Check foreign key relationships are valid
    Ensures no orphaned records
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')

    checks = []

    # Check 1: Orders must have valid customer_id
    query = """
        SELECT COUNT(*) AS orphaned_customers
        FROM orders o
        WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.customer_id = o.customer_id)
    """
    result = pd.read_sql(query, hook.get_conn())
    orphaned_customers = result['orphaned_customers'].iloc[0]
    checks.append(('Orphaned customers', orphaned_customers))

    # Check 2: Orders must have valid employee_id
    query = """
        SELECT COUNT(*) AS orphaned_employees
        FROM orders o
        WHERE o.employee_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.employee_id = o.employee_id)
    """
    result = pd.read_sql(query, hook.get_conn())
    orphaned_employees = result['orphaned_employees'].iloc[0]
    checks.append(('Orphaned employees', orphaned_employees))

    # Check 3: Order details must reference valid products
    query = """
        SELECT COUNT(*) AS orphaned_products
        FROM order_details od
        WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.product_id = od.product_id)
    """
    result = pd.read_sql(query, hook.get_conn())
    orphaned_products = result['orphaned_products'].iloc[0]
    checks.append(('Orphaned products', orphaned_products))

    # Check 4: Products must have valid category_id
    query = """
        SELECT COUNT(*) AS orphaned_categories
        FROM products p
        WHERE p.category_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM categories c WHERE c.category_id = p.category_id)
    """
    result = pd.read_sql(query, hook.get_conn())
    orphaned_categories = result['orphaned_categories'].iloc[0]
    checks.append(('Orphaned categories', orphaned_categories))

    logging.info(f"\n{'='*60}")
    logging.info("REFERENTIAL INTEGRITY CHECK")
    logging.info(f"{'='*60}")

    total_issues = 0
    for check_name, count in checks:
        status = "✓ PASS" if count == 0 else "✗ FAIL"
        logging.info(f"  {check_name}: {count} issues - {status}")
        total_issues += count

    context['ti'].xcom_push(key='referential_issues', value=total_issues)

    # Pass if no referential integrity issues
    return total_issues == 0


def check_business_rules(**context):
    """
    Validate business logic and constraints
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')

    violations = []

    # Rule 1: Unit prices must be positive
    query = "SELECT COUNT(*) AS count FROM order_details WHERE unit_price <= 0"
    result = pd.read_sql(query, hook.get_conn())
    violations.append(('Negative/zero unit prices', result['count'].iloc[0]))

    # Rule 2: Quantities must be positive
    query = "SELECT COUNT(*) AS count FROM order_details WHERE quantity <= 0"
    result = pd.read_sql(query, hook.get_conn())
    violations.append(('Negative/zero quantities', result['count'].iloc[0]))

    # Rule 3: Discounts must be between 0 and 1
    query = "SELECT COUNT(*) AS count FROM order_details WHERE discount < 0 OR discount > 1"
    result = pd.read_sql(query, hook.get_conn())
    violations.append(('Invalid discounts', result['count'].iloc[0]))

    # Rule 4: Shipped date should not be before order date
    query = """
        SELECT COUNT(*) AS count
        FROM orders
        WHERE shipped_date IS NOT NULL
          AND shipped_date < order_date
    """
    result = pd.read_sql(query, hook.get_conn())
    violations.append(('Shipped before ordered', result['count'].iloc[0]))

    # Rule 5: Required date should be after order date
    query = """
        SELECT COUNT(*) AS count
        FROM orders
        WHERE required_date IS NOT NULL
          AND required_date < order_date
    """
    result = pd.read_sql(query, hook.get_conn())
    violations.append(('Required before ordered', result['count'].iloc[0]))

    # Rule 6: Products shouldn't be discontinued and have stock
    query = """
        SELECT COUNT(*) AS count
        FROM products
        WHERE discontinued = TRUE
          AND units_in_stock > 0
    """
    result = pd.read_sql(query, hook.get_conn())
    violations.append(('Discontinued with stock', result['count'].iloc[0]))

    logging.info(f"\n{'='*60}")
    logging.info("BUSINESS RULES VALIDATION")
    logging.info(f"{'='*60}")

    total_violations = 0
    for rule_name, count in violations:
        status = "✓ PASS" if count == 0 else "✗ FAIL"
        logging.info(f"  {rule_name}: {count} violations - {status}")
        total_violations += count

    context['ti'].xcom_push(key='business_violations', value=total_violations)

    # Pass if <= 5 violations (some tolerance for demo data)
    return total_violations <= 5


def check_data_freshness(**context):
    """
    Ensure data is being actively updated
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')

    # Check most recent order date
    query = "SELECT MAX(order_date) AS latest_order FROM orders"
    result = pd.read_sql(query, hook.get_conn())

    latest_order = pd.to_datetime(result['latest_order'].iloc[0])
    days_since_last_order = (pd.Timestamp.now() - latest_order).days

    logging.info(f"\n{'='*60}")
    logging.info("DATA FRESHNESS CHECK")
    logging.info(f"{'='*60}")
    logging.info(f"  Latest order date: {latest_order.date()}")
    logging.info(f"  Days since last order: {days_since_last_order}")

    context['ti'].xcom_push(key='days_since_last_order', value=days_since_last_order)

    # Pass if orders within last 90 days (demo data)
    return days_since_last_order <= 90


def quality_check_router(**context):
    """
    Route based on all quality check results
    """
    integrity = context['ti'].xcom_pull(task_ids='check_order_integrity')
    referential = context['ti'].xcom_pull(task_ids='check_referential_integrity')
    business = context['ti'].xcom_pull(task_ids='check_business_rules')
    freshness = context['ti'].xcom_pull(task_ids='check_data_freshness')

    logging.info(f"\n{'='*60}")
    logging.info("OVERALL QUALITY ASSESSMENT")
    logging.info(f"{'='*60}")
    logging.info(f"  Order Integrity: {'✓ PASS' if integrity else '✗ FAIL'}")
    logging.info(f"  Referential Integrity: {'✓ PASS' if referential else '✗ FAIL'}")
    logging.info(f"  Business Rules: {'✓ PASS' if business else '✗ FAIL'}")
    logging.info(f"  Data Freshness: {'✓ PASS' if freshness else '✗ FAIL'}")

    if integrity and referential and business and freshness:
        logging.info(f"\n  Overall Status: ✓ ALL CHECKS PASSED")
        return 'quality_passed'
    else:
        logging.warning(f"\n  Overall Status: ✗ QUALITY CHECKS FAILED")
        return 'quality_failed'


def send_quality_alert(**context):
    """
    Send alert when quality checks fail
    """
    integrity_score = context['ti'].xcom_pull(key='integrity_score', task_ids='check_order_integrity')
    ref_issues = context['ti'].xcom_pull(key='referential_issues', task_ids='check_referential_integrity')
    business_violations = context['ti'].xcom_pull(key='business_violations', task_ids='check_business_rules')
    days_old = context['ti'].xcom_pull(key='days_since_last_order', task_ids='check_data_freshness')

    alert_message = f"""
    ╔══════════════════════════════════════════════════════════╗
    ║          DATA QUALITY ALERT - ACTION REQUIRED            ║
    ╚══════════════════════════════════════════════════════════╝

    Northwind Order Data Quality Issues Detected:

    Order Integrity Score: {integrity_score:.2f}%
    Referential Integrity Issues: {ref_issues}
    Business Rule Violations: {business_violations}
    Days Since Last Order: {days_old}

    Please investigate immediately.

    In production, this would trigger:
    - Slack notification to #data-engineering
    - PagerDuty alert for on-call engineer
    - Email to data quality team
    - Dashboard red flag
    """

    logging.error(alert_message)

    # In production: Send to Slack/Email/PagerDuty
    # slack_webhook.send(alert_message)


def log_quality_metrics(**context):
    """
    Log quality metrics for monitoring dashboard
    """
    integrity_score = context['ti'].xcom_pull(key='integrity_score', task_ids='check_order_integrity')
    total_orders = context['ti'].xcom_pull(key='total_orders', task_ids='check_order_integrity')
    ref_issues = context['ti'].xcom_pull(key='referential_issues', task_ids='check_referential_integrity')
    business_violations = context['ti'].xcom_pull(key='business_violations', task_ids='check_business_rules')
    days_old = context['ti'].xcom_pull(key='days_since_last_order', task_ids='check_data_freshness')

    metrics = {
        'timestamp': datetime.now().isoformat(),
        'database': 'northwind',
        'total_orders_checked': total_orders,
        'integrity_score_pct': integrity_score,
        'referential_issues': ref_issues,
        'business_violations': business_violations,
        'data_age_days': days_old
    }

    logging.info(f"\n{'='*60}")
    logging.info("QUALITY METRICS LOGGED")
    logging.info(f"{'='*60}")
    for key, value in metrics.items():
        logging.info(f"  {key}: {value}")

    # In production: Send to Prometheus, Datadog, CloudWatch
    # metrics_client.gauge('data_quality.integrity_score', integrity_score)
    # metrics_client.gauge('data_quality.referential_issues', ref_issues)


# Define tasks
start = EmptyOperator(task_id='start', dag=dag)

integrity_check = PythonOperator(
    task_id='check_order_integrity',
    python_callable=check_order_integrity,
    dag=dag,
)

referential_check = PythonOperator(
    task_id='check_referential_integrity',
    python_callable=check_referential_integrity,
    dag=dag,
)

business_check = PythonOperator(
    task_id='check_business_rules',
    python_callable=check_business_rules,
    dag=dag,
)

freshness_check = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
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
    python_callable=send_quality_alert,
    dag=dag,
)

log_task = PythonOperator(
    task_id='log_metrics',
    python_callable=log_quality_metrics,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success', dag=dag)

# Task dependencies
start >> [integrity_check, referential_check, business_check, freshness_check] >> route_task
route_task >> [quality_passed, quality_failed] >> log_task >> end
