"""
Customer and Product Analytics with PySpark
Analyzes Northwind order data using distributed processing
Demonstrates: Local cluster simulation, complex joins, window functions, aggregations
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'customer_product_analytics',
    default_args=default_args,
    description='Advanced analytics on Northwind data using PySpark local cluster',
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'pyspark', 'analytics', 'northwind', 'customers', 'products'],
)


def extract_northwind_data(**context):
    """
    Extract Northwind data from PostgreSQL
    Prepares datasets for PySpark processing
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()

    import pandas as pd

    # Extract orders with customer and employee info
    orders_query = """
        SELECT
            o.order_id,
            o.customer_id,
            c.company_name AS customer_name,
            c.country AS customer_country,
            c.city AS customer_city,
            o.employee_id,
            e.first_name || ' ' || e.last_name AS employee_name,
            o.order_date,
            o.shipped_date,
            o.freight
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN employees e ON o.employee_id = e.employee_id
        ORDER BY o.order_date DESC
        LIMIT 500
    """
    orders_df = pd.read_sql(orders_query, conn)

    # Extract order details with product info
    details_query = """
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
        WHERE od.order_id IN (
            SELECT order_id FROM orders ORDER BY order_date DESC LIMIT 500
        )
    """
    details_df = pd.read_sql(details_query, conn)

    # Extract product catalog
    products_query = """
        SELECT
            p.product_id,
            p.product_name,
            c.category_name,
            p.unit_price,
            p.units_in_stock,
            p.units_on_order,
            p.reorder_level,
            p.discontinued
        FROM products p
        JOIN categories c ON p.category_id = c.category_id
    """
    products_df = pd.read_sql(products_query, conn)

    conn.close()

    logging.info(f"{'='*60}")
    logging.info("DATA EXTRACTION COMPLETED")
    logging.info(f"{'='*60}")
    logging.info(f"  Orders extracted: {len(orders_df)}")
    logging.info(f"  Order line items: {len(details_df)}")
    logging.info(f"  Products in catalog: {len(products_df)}")

    # Push to XCom as JSON for PySpark tasks
    context['ti'].xcom_push(key='orders_data', value=orders_df.to_json(orient='records'))
    context['ti'].xcom_push(key='details_data', value=details_df.to_json(orient='records'))
    context['ti'].xcom_push(key='products_data', value=products_df.to_json(orient='records'))


def analyze_customer_behavior(**context):
    """
    Analyze customer purchase patterns using PySpark
    Demonstrates: RFM analysis, customer segmentation, window functions
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum as _sum, avg, count, max as _max, min as _min
    from pyspark.sql.functions import datediff, current_date, round as spark_round
    from pyspark.sql.functions import row_number, ntile
    from pyspark.sql.window import Window
    import json
    import pandas as pd

    # Create SparkSession with 4 cores (simulates 4-node cluster)
    spark = SparkSession.builder \
        .appName("NorthwindCustomerAnalytics") \
        .master("local[4]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    logging.info(f"{'='*60}")
    logging.info("PYSPARK CUSTOMER ANALYTICS SESSION")
    logging.info(f"{'='*60}")
    logging.info(f"  Spark Version: {spark.version}")
    logging.info(f"  Execution Mode: local[4] (4 cores)")
    logging.info(f"  Shuffle Partitions: 8")
    logging.info(f"{'='*60}")

    try:
        # Load data from XCom
        orders_json = context['ti'].xcom_pull(key='orders_data', task_ids='extract_data')
        details_json = context['ti'].xcom_pull(key='details_data', task_ids='extract_data')

        orders_pd = pd.read_json(orders_json)
        details_pd = pd.read_json(details_json)

        # Create Spark DataFrames with partitioning
        orders_df = spark.createDataFrame(orders_pd).repartition(8, "customer_id")
        details_df = spark.createDataFrame(details_pd).repartition(8, "order_id")

        logging.info(f"\n1. DataFrames Created:")
        logging.info(f"   Orders: {orders_df.count()} rows, {orders_df.rdd.getNumPartitions()} partitions")
        logging.info(f"   Details: {details_df.count()} rows, {details_df.rdd.getNumPartitions()} partitions")

        # Join orders with details to get complete transaction view
        customer_orders = orders_df \
            .join(details_df, "order_id", "inner") \
            .select(
                "customer_id",
                "customer_name",
                "customer_country",
                "customer_city",
                "order_id",
                "order_date",
                "product_id",
                "product_name",
                "category_name",
                "quantity",
                "extended_price"
            )

        logging.info(f"\n2. Joined Transaction View:")
        customer_orders.show(5, truncate=False)

        # Customer-level aggregations (RFM-style analysis)
        customer_metrics = customer_orders \
            .groupBy("customer_id", "customer_name", "customer_country") \
            .agg(
                count("order_id").alias("total_orders"),
                _sum("extended_price").alias("total_revenue"),
                avg("extended_price").alias("avg_order_line_value"),
                _max("order_date").alias("last_order_date"),
                _min("order_date").alias("first_order_date"),
                _sum("quantity").alias("total_units_purchased")
            ) \
            .withColumn("avg_order_value",
                       spark_round(col("total_revenue") / col("total_orders"), 2)) \
            .withColumn("days_since_last_order",
                       datediff(current_date(), col("last_order_date"))) \
            .orderBy(col("total_revenue").desc())

        logging.info(f"\n3. Customer Metrics (Top 10 by Revenue):")
        customer_metrics.show(10, truncate=False)

        # Customer Segmentation using window functions
        windowSpec = Window.orderBy(col("total_revenue").desc())

        customer_segmentation = customer_metrics \
            .withColumn("revenue_rank", row_number().over(windowSpec)) \
            .withColumn("revenue_quartile", ntile(4).over(windowSpec))

        logging.info(f"\n4. Customer Segmentation (Revenue Quartiles):")
        customer_segmentation.select(
            "customer_name",
            "customer_country",
            "total_revenue",
            "total_orders",
            "revenue_rank",
            "revenue_quartile"
        ).show(15, truncate=False)

        # Geographic Analysis
        country_performance = customer_metrics \
            .groupBy("customer_country") \
            .agg(
                count("customer_id").alias("customer_count"),
                _sum("total_revenue").alias("country_revenue"),
                avg("total_revenue").alias("avg_customer_value"),
                _sum("total_orders").alias("total_orders")
            ) \
            .withColumn("revenue_per_order",
                       spark_round(col("country_revenue") / col("total_orders"), 2)) \
            .orderBy(col("country_revenue").desc())

        logging.info(f"\n5. Country Performance Analysis:")
        country_performance.show(10, truncate=False)

        # Product Category Preferences by Customer Segment
        top_customers = customer_segmentation \
            .filter(col("revenue_quartile") == 1) \
            .select("customer_id")

        top_customer_categories = customer_orders \
            .join(top_customers, "customer_id", "inner") \
            .groupBy("category_name") \
            .agg(
                _sum("extended_price").alias("category_revenue"),
                count("order_id").alias("order_count"),
                _sum("quantity").alias("units_sold")
            ) \
            .orderBy(col("category_revenue").desc())

        logging.info(f"\n6. Top Customer Category Preferences:")
        top_customer_categories.show()

        # Calculate key metrics for XCom
        total_customers = customer_metrics.count()
        total_revenue = customer_metrics.agg(_sum("total_revenue")).collect()[0][0]
        avg_customer_value = customer_metrics.agg(avg("total_revenue")).collect()[0][0]
        top_country = country_performance.first()

        # Push metrics to XCom
        context['ti'].xcom_push(key='total_customers', value=total_customers)
        context['ti'].xcom_push(key='total_revenue', value=float(total_revenue))
        context['ti'].xcom_push(key='avg_customer_value', value=float(avg_customer_value))
        context['ti'].xcom_push(key='top_country', value=top_country["customer_country"])
        context['ti'].xcom_push(key='top_country_revenue', value=float(top_country["country_revenue"]))

        logging.info(f"\n{'='*60}")
        logging.info("CUSTOMER ANALYTICS SUMMARY")
        logging.info(f"{'='*60}")
        logging.info(f"  Total Customers Analyzed: {total_customers}")
        logging.info(f"  Total Revenue: ${total_revenue:,.2f}")
        logging.info(f"  Average Customer Value: ${avg_customer_value:,.2f}")
        logging.info(f"  Top Country: {top_country['customer_country']} (${top_country['country_revenue']:,.2f})")
        logging.info(f"{'='*60}")

    finally:
        spark.stop()
        logging.info("Spark session stopped successfully")


def analyze_product_performance(**context):
    """
    Analyze product sales and profitability using PySpark
    Demonstrates: Complex aggregations, inventory analysis, trend detection
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum as _sum, avg, count, round as spark_round
    from pyspark.sql.functions import when
    import json
    import pandas as pd

    spark = SparkSession.builder \
        .appName("NorthwindProductAnalytics") \
        .master("local[4]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    logging.info("Running Product Performance Analytics")

    try:
        # Load data from XCom
        details_json = context['ti'].xcom_pull(key='details_data', task_ids='extract_data')
        products_json = context['ti'].xcom_pull(key='products_data', task_ids='extract_data')

        details_pd = pd.read_json(details_json)
        products_pd = pd.read_json(products_json)

        # Create Spark DataFrames
        details_df = spark.createDataFrame(details_pd).repartition(8, "product_id")
        products_df = spark.createDataFrame(products_pd)

        logging.info(f"\n1. Product DataFrames Loaded:")
        logging.info(f"   Order Details: {details_df.count()} line items")
        logging.info(f"   Product Catalog: {products_df.count()} products")

        # Product Sales Performance
        product_sales = details_df \
            .groupBy("product_id", "product_name", "category_name") \
            .agg(
                count("order_id").alias("times_ordered"),
                _sum("quantity").alias("total_units_sold"),
                _sum("extended_price").alias("total_revenue"),
                avg("unit_price").alias("avg_selling_price"),
                avg("discount").alias("avg_discount_rate")
            ) \
            .withColumn("revenue_per_unit",
                       spark_round(col("total_revenue") / col("total_units_sold"), 2)) \
            .orderBy(col("total_revenue").desc())

        logging.info(f"\n2. Product Sales Performance (Top 10):")
        product_sales.show(10, truncate=False)

        # Join with inventory data
        product_analysis = product_sales \
            .join(products_df.select("product_id", "units_in_stock", "units_on_order", "reorder_level"),
                  "product_id", "left") \
            .withColumn("inventory_status",
                when(col("units_in_stock") <= col("reorder_level"), "LOW STOCK")
                .when(col("units_in_stock") > col("reorder_level") * 3, "OVERSTOCKED")
                .otherwise("NORMAL")) \
            .withColumn("stock_coverage_orders",
                       spark_round(col("units_in_stock") / (col("total_units_sold") / 30), 1))

        logging.info(f"\n3. Product Analysis with Inventory:")
        product_analysis.select(
            "product_name",
            "total_revenue",
            "total_units_sold",
            "units_in_stock",
            "inventory_status",
            "stock_coverage_orders"
        ).show(15, truncate=False)

        # Category Performance
        category_performance = product_sales \
            .groupBy("category_name") \
            .agg(
                count("product_id").alias("products_in_category"),
                _sum("total_revenue").alias("category_revenue"),
                _sum("total_units_sold").alias("category_units_sold"),
                avg("avg_discount_rate").alias("avg_category_discount")
            ) \
            .withColumn("revenue_per_product",
                       spark_round(col("category_revenue") / col("products_in_category"), 2)) \
            .orderBy(col("category_revenue").desc())

        logging.info(f"\n4. Category Performance:")
        category_performance.show()

        # Identify low stock items that are selling well
        stock_alerts = product_analysis \
            .filter(col("inventory_status") == "LOW STOCK") \
            .filter(col("total_revenue") > 1000) \
            .select(
                "product_name",
                "category_name",
                "total_revenue",
                "total_units_sold",
                "units_in_stock",
                "units_on_order"
            ) \
            .orderBy(col("total_revenue").desc())

        logging.info(f"\n5. Stock Alerts (High-revenue products with low inventory):")
        stock_alert_count = stock_alerts.count()
        if stock_alert_count > 0:
            stock_alerts.show(truncate=False)
            logging.info(f"   ⚠ {stock_alert_count} products need restocking attention")
        else:
            logging.info("   ✓ No critical stock alerts")

        # Calculate metrics for XCom
        total_products = product_sales.count()
        total_product_revenue = product_sales.agg(_sum("total_revenue")).collect()[0][0]
        top_product = product_sales.first()
        top_category = category_performance.first()

        context['ti'].xcom_push(key='total_products', value=total_products)
        context['ti'].xcom_push(key='total_product_revenue', value=float(total_product_revenue))
        context['ti'].xcom_push(key='top_product', value=top_product["product_name"])
        context['ti'].xcom_push(key='top_product_revenue', value=float(top_product["total_revenue"]))
        context['ti'].xcom_push(key='top_category', value=top_category["category_name"])
        context['ti'].xcom_push(key='stock_alerts', value=stock_alert_count)

        logging.info(f"\n{'='*60}")
        logging.info("PRODUCT ANALYTICS SUMMARY")
        logging.info(f"{'='*60}")
        logging.info(f"  Products Analyzed: {total_products}")
        logging.info(f"  Total Product Revenue: ${total_product_revenue:,.2f}")
        logging.info(f"  Top Product: {top_product['product_name']} (${top_product['total_revenue']:,.2f})")
        logging.info(f"  Top Category: {top_category['category_name']} (${top_category['category_revenue']:,.2f})")
        logging.info(f"  Stock Alerts: {stock_alert_count}")
        logging.info(f"{'='*60}")

    finally:
        spark.stop()
        logging.info("Product analytics completed")


def generate_executive_summary(**context):
    """
    Pull all analytics results and generate executive summary
    """
    # Customer metrics
    total_customers = context['ti'].xcom_pull(key='total_customers', task_ids='customer_analytics')
    customer_revenue = context['ti'].xcom_pull(key='total_revenue', task_ids='customer_analytics')
    avg_customer_value = context['ti'].xcom_pull(key='avg_customer_value', task_ids='customer_analytics')
    top_country = context['ti'].xcom_pull(key='top_country', task_ids='customer_analytics')
    top_country_revenue = context['ti'].xcom_pull(key='top_country_revenue', task_ids='customer_analytics')

    # Product metrics
    total_products = context['ti'].xcom_pull(key='total_products', task_ids='product_analytics')
    product_revenue = context['ti'].xcom_pull(key='total_product_revenue', task_ids='product_analytics')
    top_product = context['ti'].xcom_pull(key='top_product', task_ids='product_analytics')
    top_product_revenue = context['ti'].xcom_pull(key='top_product_revenue', task_ids='product_analytics')
    top_category = context['ti'].xcom_pull(key='top_category', task_ids='product_analytics')
    stock_alerts = context['ti'].xcom_pull(key='stock_alerts', task_ids='product_analytics')

    summary = f"""
    ╔══════════════════════════════════════════════════════════════╗
    ║         NORTHWIND ANALYTICS EXECUTIVE SUMMARY                ║
    ║              Powered by Apache Spark (Local Cluster)         ║
    ╚══════════════════════════════════════════════════════════════╝

    CUSTOMER INSIGHTS:
    ─────────────────────────────────────────────────────────────────
      📊 Total Active Customers: {total_customers}
      💰 Total Customer Revenue: ${customer_revenue:,.2f}
      📈 Average Customer Value: ${avg_customer_value:,.2f}
      🌍 Top Market: {top_country} (${top_country_revenue:,.2f})

    PRODUCT PERFORMANCE:
    ─────────────────────────────────────────────────────────────────
      🛍️  Active Products: {total_products}
      💵 Total Product Revenue: ${product_revenue:,.2f}
      ⭐ Best-Selling Product: {top_product} (${top_product_revenue:,.2f})
      📂 Top Category: {top_category}
      ⚠️  Stock Alerts: {stock_alerts} products need attention

    TECHNICAL IMPLEMENTATION:
    ─────────────────────────────────────────────────────────────────
      🔧 Processing Engine: Apache Spark (PySpark)
      🖥️  Execution Mode: local[4] (4-core cluster simulation)
      📦 Partitioning: 8 partitions across 4 cores
      🔄 Operations: Multi-table joins, window functions, aggregations
      📊 Techniques: RFM analysis, customer segmentation, inventory optimization

    KEY CAPABILITIES DEMONSTRATED:
    ─────────────────────────────────────────────────────────────────
      ✓ Distributed data processing without infrastructure
      ✓ Complex multi-table joins across Northwind schema
      ✓ Customer behavior analysis and segmentation
      ✓ Product profitability and inventory analytics
      ✓ Geographic performance analysis
      ✓ Real-time stock alert generation

    ╔══════════════════════════════════════════════════════════════╗
    ║  Interview Ready: Demonstrates production Spark patterns     ║
    ║  Portfolio Highlight: Real business analytics at scale       ║
    ╚══════════════════════════════════════════════════════════════╝
    """

    logging.info(summary)


# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_northwind_data,
    dag=dag,
)

customer_analytics_task = PythonOperator(
    task_id='customer_analytics',
    python_callable=analyze_customer_behavior,
    dag=dag,
)

product_analytics_task = PythonOperator(
    task_id='product_analytics',
    python_callable=analyze_product_performance,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='executive_summary',
    python_callable=generate_executive_summary,
    dag=dag,
)

# Task dependencies
extract_task >> [customer_analytics_task, product_analytics_task] >> summary_task
