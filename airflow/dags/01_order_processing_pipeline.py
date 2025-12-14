"""
Order Processing ETL Pipeline
Processes Northwind orders to generate daily sales analytics
Demonstrates: Extract, Transform, Load pattern with real business data
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

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
    'order_processing_pipeline',
    default_args=default_args,
    description='ETL pipeline processing Northwind orders for daily analytics',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'northwind', 'orders', 'analytics'],
)


def extract_orders(**context):
    """
    Extract yesterday's shipped orders from Northwind database
    Business Rule: Only process orders that have been shipped
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')

    # Extract orders with customer and employee details
    query = """
        SELECT
            o.order_id,
            o.customer_id,
            c.company_name AS customer_name,
            c.country AS customer_country,
            o.employee_id,
            e.first_name || ' ' || e.last_name AS employee_name,
            o.order_date,
            o.shipped_date,
            o.freight,
            o.ship_country,
            s.company_name AS shipper_name
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN employees e ON o.employee_id = e.employee_id
        JOIN shippers s ON o.ship_via = s.shipper_id
        WHERE o.shipped_date >= CURRENT_DATE - INTERVAL '1 day'
          AND o.shipped_date < CURRENT_DATE
          AND o.shipped_date IS NOT NULL
        ORDER BY o.order_id
    """

    df = pd.read_sql(query, hook.get_conn())

    logging.info(f"✓ Extracted {len(df)} shipped orders from yesterday")

    if len(df) == 0:
        logging.warning("No orders shipped yesterday - this is expected for demo data")
        # For demo: extract last 7 days if yesterday is empty
        query_last_week = query.replace('1 day', '7 days').replace('< CURRENT_DATE', '< CURRENT_DATE + INTERVAL \'1 day\'')
        df = pd.read_sql(query_last_week, hook.get_conn())
        logging.info(f"✓ Using last 7 days instead: {len(df)} orders")

    # Save to XCom
    context['ti'].xcom_push(key='orders_data', value=df.to_json(orient='records'))
    context['ti'].xcom_push(key='order_count', value=len(df))


def extract_order_details(**context):
    """
    Extract line items for the orders
    Calculates extended price for each line item
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')

    # Get order IDs from previous task
    orders_json = context['ti'].xcom_pull(key='orders_data', task_ids='extract_orders')

    if not orders_json:
        logging.warning("No orders to process")
        context['ti'].xcom_push(key='order_details_data', value='[]')
        return

    orders_df = pd.read_json(orders_json)
    order_ids = tuple(orders_df['order_id'].tolist())

    # Extract order details with product information
    query = f"""
        SELECT
            od.order_id,
            od.product_id,
            p.product_name,
            c.category_name,
            od.unit_price,
            od.quantity,
            od.discount,
            (od.unit_price * od.quantity * (1 - od.discount)) AS extended_price
        FROM order_details od
        JOIN products p ON od.product_id = p.product_id
        JOIN categories c ON p.category_id = c.category_id
        WHERE od.order_id IN {order_ids if len(order_ids) > 1 else f'({order_ids[0]})'}
    """

    df = pd.read_sql(query, hook.get_conn())

    logging.info(f"✓ Extracted {len(df)} order line items")
    logging.info(f"  Total Revenue: ${df['extended_price'].sum():,.2f}")
    logging.info(f"  Unique Products: {df['product_id'].nunique()}")

    context['ti'].xcom_push(key='order_details_data', value=df.to_json(orient='records'))


def transform_sales_data(**context):
    """
    Transform orders into analytics-ready datasets
    Creates multiple aggregations for different business views
    """
    # Get data from previous tasks
    orders_json = context['ti'].xcom_pull(key='orders_data', task_ids='extract_orders')
    details_json = context['ti'].xcom_pull(key='order_details_data', task_ids='extract_order_details')

    if not orders_json or not details_json:
        logging.warning("No data to transform")
        return

    orders_df = pd.read_json(orders_json)
    details_df = pd.read_json(details_json)

    # 1. Daily Sales Summary
    daily_summary = {
        'date': datetime.now().date(),
        'total_orders': len(orders_df),
        'total_revenue': float(details_df['extended_price'].sum()),
        'total_items_sold': int(details_df['quantity'].sum()),
        'unique_customers': int(orders_df['customer_id'].nunique()),
        'unique_products': int(details_df['product_id'].nunique()),
        'avg_order_value': float(details_df.groupby('order_id')['extended_price'].sum().mean()),
        'avg_discount': float(details_df['discount'].mean()),
        'total_freight': float(orders_df['freight'].sum())
    }

    logging.info("\n" + "="*60)
    logging.info("DAILY SALES SUMMARY")
    logging.info("="*60)
    for key, value in daily_summary.items():
        if isinstance(value, float):
            logging.info(f"  {key}: ${value:,.2f}" if 'revenue' in key or 'freight' in key or 'value' in key else f"  {key}: {value:.2%}" if 'discount' in key else f"  {key}: {value:,.2f}")
        else:
            logging.info(f"  {key}: {value}")

    # 2. Sales by Category
    category_sales = details_df.groupby('category_name').agg({
        'extended_price': 'sum',
        'quantity': 'sum',
        'order_id': 'nunique'
    }).reset_index()
    category_sales.columns = ['category_name', 'revenue', 'units_sold', 'order_count']
    category_sales['avg_item_price'] = category_sales['revenue'] / category_sales['units_sold']
    category_sales = category_sales.sort_values('revenue', ascending=False)

    logging.info("\nSALES BY CATEGORY (Top 5):")
    logging.info(category_sales.head().to_string(index=False))

    # 3. Sales by Country
    # Merge orders with details to get country information
    sales_by_country = orders_df.merge(
        details_df.groupby('order_id')['extended_price'].sum().reset_index(),
        on='order_id'
    ).groupby('customer_country').agg({
        'extended_price': 'sum',
        'order_id': 'count',
        'customer_id': 'nunique'
    }).reset_index()
    sales_by_country.columns = ['country', 'revenue', 'order_count', 'customer_count']
    sales_by_country = sales_by_country.sort_values('revenue', ascending=False)

    logging.info("\nSALES BY COUNTRY (Top 5):")
    logging.info(sales_by_country.head().to_string(index=False))

    # 4. Top Employees
    employee_sales = orders_df.merge(
        details_df.groupby('order_id')['extended_price'].sum().reset_index(),
        on='order_id'
    ).groupby('employee_name').agg({
        'extended_price': 'sum',
        'order_id': 'count'
    }).reset_index()
    employee_sales.columns = ['employee_name', 'revenue', 'order_count']
    employee_sales = employee_sales.sort_values('revenue', ascending=False)

    logging.info("\nTOP EMPLOYEES:")
    logging.info(employee_sales.to_string(index=False))

    # Save transformed data
    context['ti'].xcom_push(key='daily_summary', value=daily_summary)
    context['ti'].xcom_push(key='category_sales', value=category_sales.to_json(orient='records'))
    context['ti'].xcom_push(key='country_sales', value=sales_by_country.to_json(orient='records'))
    context['ti'].xcom_push(key='employee_sales', value=employee_sales.to_json(orient='records'))


def load_analytics(**context):
    """
    Load transformed data into analytics tables
    Creates denormalized tables optimized for reporting
    """
    daily_summary = context['ti'].xcom_pull(key='daily_summary', task_ids='transform')
    category_sales_json = context['ti'].xcom_pull(key='category_sales', task_ids='transform')
    country_sales_json = context['ti'].xcom_pull(key='country_sales', task_ids='transform')

    if not daily_summary:
        logging.warning("No data to load")
        return

    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()

    # Load daily summary
    summary_df = pd.DataFrame([daily_summary])
    summary_df.to_sql(
        'daily_sales_summary',
        engine,
        schema='analytics',
        if_exists='append',
        index=False
    )
    logging.info(f"✓ Loaded daily summary to analytics.daily_sales_summary")

    # Load category sales
    if category_sales_json:
        category_df = pd.read_json(category_sales_json)
        category_df['date'] = datetime.now().date()
        category_df.to_sql(
            'daily_category_sales',
            engine,
            schema='analytics',
            if_exists='append',
            index=False
        )
        logging.info(f"✓ Loaded {len(category_df)} category records to analytics.daily_category_sales")

    # Load country sales
    if country_sales_json:
        country_df = pd.read_json(country_sales_json)
        country_df['date'] = datetime.now().date()
        country_df.to_sql(
            'daily_country_sales',
            engine,
            schema='analytics',
            if_exists='append',
            index=False
        )
        logging.info(f"✓ Loaded {len(country_df)} country records to analytics.daily_country_sales")

    logging.info("\n" + "="*60)
    logging.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
    logging.info("="*60)


# Create analytics schema and tables
create_analytics_schema = PostgresOperator(
    task_id='create_analytics_schema',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE SCHEMA IF NOT EXISTS analytics;

        CREATE TABLE IF NOT EXISTS analytics.daily_sales_summary (
            date DATE PRIMARY KEY,
            total_orders INTEGER,
            total_revenue DECIMAL(10,2),
            total_items_sold INTEGER,
            unique_customers INTEGER,
            unique_products INTEGER,
            avg_order_value DECIMAL(10,2),
            avg_discount DECIMAL(5,4),
            total_freight DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS analytics.daily_category_sales (
            date DATE,
            category_name VARCHAR(50),
            revenue DECIMAL(10,2),
            units_sold INTEGER,
            order_count INTEGER,
            avg_item_price DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, category_name)
        );

        CREATE TABLE IF NOT EXISTS analytics.daily_country_sales (
            date DATE,
            country VARCHAR(50),
            revenue DECIMAL(10,2),
            order_count INTEGER,
            customer_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, country)
        );

        CREATE INDEX IF NOT EXISTS idx_daily_sales_date ON analytics.daily_sales_summary(date);
        CREATE INDEX IF NOT EXISTS idx_category_sales_date ON analytics.daily_category_sales(date);
        CREATE INDEX IF NOT EXISTS idx_country_sales_date ON analytics.daily_country_sales(date);
    """,
    dag=dag,
)

# Define tasks
extract_orders_task = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders,
    dag=dag,
)

extract_details_task = PythonOperator(
    task_id='extract_order_details',
    python_callable=extract_order_details,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_sales_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_analytics,
    dag=dag,
)

# Task dependencies
create_analytics_schema >> extract_orders_task >> extract_details_task >> transform_task >> load_task
