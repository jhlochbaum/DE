"""
PySpark Local Processing DAG
Demonstrates distributed data processing using PySpark in local mode
Uses multiple cores to simulate a Spark cluster
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    'pyspark_local_processing',
    default_args=default_args,
    description='Process data using PySpark in local mode (multi-core)',
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'pyspark', 'distributed-processing', 'analytics'],
)


def run_spark_aggregation(**context):
    """
    Run PySpark job in local mode with multiple cores
    Demonstrates: DataFrames, aggregations, partitioning, window functions
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum as _sum, avg, count, max as _max, min as _min
    from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead
    from pyspark.sql.window import Window

    # Create SparkSession in local mode with 4 cores (simulates 4-node cluster)
    spark = SparkSession.builder \
        .appName("AirflowPySparkLocal") \
        .master("local[4]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    logging.info("=" * 60)
    logging.info("PySpark Session Started in Local Mode with 4 cores")
    logging.info(f"Spark Version: {spark.version}")
    logging.info("=" * 60)

    try:
        # Read data from PostgreSQL
        jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
        connection_properties = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver"
        }

        # For demo: Create sample data instead of reading from DB (avoids JDBC driver issues)
        # In production, you'd read from actual database or data lake

        # Create sample sales data
        sales_data = [
            (1, 101, 1, 1299.99, "2024-01-15", "completed"),
            (2, 102, 2, 29.99, "2024-01-15", "completed"),
            (3, 101, 3, 199.99, "2024-01-16", "completed"),
            (4, 103, 1, 1299.99, "2024-01-17", "completed"),
            (5, 102, 4, 499.99, "2024-01-18", "completed"),
            (6, 101, 2, 29.99, "2024-01-19", "completed"),
            (7, 104, 5, 49.99, "2024-01-20", "failed"),
            (8, 103, 3, 199.99, "2024-01-21", "completed"),
            (9, 105, 1, 1299.99, "2024-01-22", "completed"),
            (10, 104, 4, 499.99, "2024-01-23", "completed"),
        ]

        columns = ["transaction_id", "user_id", "product_id", "amount", "transaction_date", "status"]

        # Create DataFrame with 8 partitions (distributed across 4 cores)
        df = spark.createDataFrame(sales_data, columns).repartition(8)

        logging.info(f"\n1. Created DataFrame with {df.count()} transactions")
        logging.info(f"   Partitions: {df.rdd.getNumPartitions()}")
        df.show(5)

        # Transformation 1: Filter and aggregate by user
        user_metrics = df.filter(col("status") == "completed") \
            .groupBy("user_id") \
            .agg(
                count("transaction_id").alias("total_transactions"),
                _sum("amount").alias("total_spent"),
                avg("amount").alias("avg_transaction"),
                _max("amount").alias("max_transaction"),
                _min("amount").alias("min_transaction")
            ) \
            .orderBy(col("total_spent").desc())

        logging.info("\n2. User Aggregations (Completed Transactions Only):")
        user_metrics.show()

        # Transformation 2: Window functions (demonstrates advanced Spark)
        windowSpec = Window.partitionBy("user_id").orderBy("transaction_date")

        df_with_windows = df.filter(col("status") == "completed") \
            .withColumn("transaction_number", row_number().over(windowSpec)) \
            .withColumn("rank_by_date", rank().over(windowSpec)) \
            .withColumn("previous_amount", lag("amount", 1).over(windowSpec)) \
            .withColumn("next_amount", lead("amount", 1).over(windowSpec))

        logging.info("\n3. Window Functions (Transaction sequence per user):")
        df_with_windows.select(
            "user_id", "transaction_date", "amount",
            "transaction_number", "previous_amount", "next_amount"
        ).show(10)

        # Transformation 3: Product performance
        product_stats = df.filter(col("status") == "completed") \
            .groupBy("product_id") \
            .agg(
                count("*").alias("units_sold"),
                _sum("amount").alias("revenue"),
                avg("amount").alias("avg_price")
            ) \
            .orderBy(col("revenue").desc())

        logging.info("\n4. Product Performance:")
        product_stats.show()

        # Demonstrate partitioning strategy
        logging.info(f"\n5. Partitioning Info:")
        logging.info(f"   Original partitions: {df.rdd.getNumPartitions()}")
        logging.info(f"   Records per partition: {df.rdd.glom().map(len).collect()}")

        # Coalesce for output (demonstrates partition management)
        final_output = user_metrics.coalesce(1)
        logging.info(f"   Final output partitions: {final_output.rdd.getNumPartitions()}")

        # Push key metrics to XCom for downstream tasks
        total_revenue = df.filter(col("status") == "completed") \
            .agg(_sum("amount").alias("total")).collect()[0]["total"]

        total_transactions = df.filter(col("status") == "completed").count()

        context['ti'].xcom_push(key='total_revenue', value=float(total_revenue))
        context['ti'].xcom_push(key='total_transactions', value=total_transactions)
        context['ti'].xcom_push(key='unique_users', value=user_metrics.count())

        logging.info("\n" + "=" * 60)
        logging.info("PySpark Processing Summary:")
        logging.info(f"  Total Revenue: ${total_revenue:,.2f}")
        logging.info(f"  Total Transactions: {total_transactions}")
        logging.info(f"  Unique Users: {user_metrics.count()}")
        logging.info("=" * 60)

    finally:
        spark.stop()
        logging.info("\nSpark session stopped successfully")


def run_spark_transformations(**context):
    """
    Advanced PySpark transformations
    Demonstrates: Joins, complex aggregations, data quality
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, concat, lit, round as spark_round

    spark = SparkSession.builder \
        .appName("AirflowPySparkTransformations") \
        .master("local[4]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    logging.info("Running Advanced PySpark Transformations")

    try:
        # Sample datasets
        users_data = [
            (101, "Alice", "alice@example.com", "USA"),
            (102, "Bob", "bob@example.com", "Canada"),
            (103, "Charlie", "charlie@example.com", "UK"),
            (104, "Diana", "diana@example.com", "USA"),
            (105, "Eve", "eve@example.com", "Germany"),
        ]

        products_data = [
            (1, "Laptop Pro", "Electronics", 1299.99),
            (2, "Wireless Mouse", "Electronics", 29.99),
            (3, "Desk Chair", "Furniture", 199.99),
            (4, "Standing Desk", "Furniture", 499.99),
            (5, "USB-C Hub", "Electronics", 49.99),
        ]

        transactions_data = [
            (1, 101, 1, 1299.99, "completed"),
            (2, 102, 2, 29.99, "completed"),
            (3, 101, 3, 199.99, "completed"),
            (4, 103, 1, 1299.99, "completed"),
            (5, 102, 4, 499.99, "completed"),
        ]

        # Create DataFrames
        users_df = spark.createDataFrame(users_data, ["user_id", "name", "email", "country"])
        products_df = spark.createDataFrame(products_data, ["product_id", "name", "category", "price"])
        transactions_df = spark.createDataFrame(transactions_data,
                                                ["transaction_id", "user_id", "product_id", "amount", "status"])

        # Complex Join (demonstrates multi-table joins)
        enriched_transactions = transactions_df \
            .join(users_df, "user_id", "left") \
            .join(products_df, "product_id", "left") \
            .select(
                transactions_df.transaction_id,
                users_df.name.alias("customer_name"),
                users_df.country,
                products_df.name.alias("product_name"),
                products_df.category,
                transactions_df.amount,
                transactions_df.status
            )

        logging.info("\n1. Enriched Transactions (Multi-table Join):")
        enriched_transactions.show(truncate=False)

        # Business Logic Transformations
        categorized_transactions = enriched_transactions \
            .withColumn("transaction_tier",
                when(col("amount") >= 1000, "High Value")
                .when(col("amount") >= 100, "Medium Value")
                .otherwise("Low Value")) \
            .withColumn("customer_display",
                concat(col("customer_name"), lit(" ("), col("country"), lit(")")))

        logging.info("\n2. Categorized Transactions:")
        categorized_transactions.select(
            "customer_display", "product_name", "amount", "transaction_tier"
        ).show(truncate=False)

        # Aggregation by multiple dimensions
        category_country_stats = enriched_transactions \
            .groupBy("category", "country") \
            .agg({
                "amount": "sum",
                "transaction_id": "count"
            }) \
            .withColumnRenamed("sum(amount)", "total_revenue") \
            .withColumnRenamed("count(transaction_id)", "transaction_count") \
            .withColumn("avg_transaction_value",
                spark_round(col("total_revenue") / col("transaction_count"), 2)) \
            .orderBy("total_revenue", ascending=False)

        logging.info("\n3. Category x Country Performance:")
        category_country_stats.show()

        # Push results to XCom
        top_category = category_country_stats.first()
        context['ti'].xcom_push(key='top_category', value=top_category["category"])
        context['ti'].xcom_push(key='top_country', value=top_category["country"])

    finally:
        spark.stop()
        logging.info("Advanced transformations completed")


def summarize_results(**context):
    """Pull results from XCom and log summary"""
    total_revenue = context['ti'].xcom_pull(key='total_revenue', task_ids='spark_aggregation')
    total_transactions = context['ti'].xcom_pull(key='total_transactions', task_ids='spark_aggregation')
    unique_users = context['ti'].xcom_pull(key='unique_users', task_ids='spark_aggregation')
    top_category = context['ti'].xcom_pull(key='top_category', task_ids='spark_transformations')
    top_country = context['ti'].xcom_pull(key='top_country', task_ids='spark_transformations')

    summary = f"""
    ╔══════════════════════════════════════════════════════════╗
    ║          PySpark Processing Summary                      ║
    ╚══════════════════════════════════════════════════════════╝

    Revenue Metrics:
      • Total Revenue: ${total_revenue:,.2f}
      • Total Transactions: {total_transactions}
      • Unique Customers: {unique_users}
      • Average Transaction: ${total_revenue/total_transactions:,.2f}

    Top Performers:
      • Best Category: {top_category}
      • Best Country: {top_country}

    Technical Details:
      • Execution Mode: PySpark Local[4] (4 cores)
      • Partitioning: 8 partitions across 4 cores
      • Operations: Aggregations, Joins, Window Functions
      • Data Quality: Filtered failed transactions

    ╔══════════════════════════════════════════════════════════╗
    ║  Demonstrates: Distributed Processing Without Cluster    ║
    ╚══════════════════════════════════════════════════════════╝
    """

    logging.info(summary)


# Define tasks
aggregation_task = PythonOperator(
    task_id='spark_aggregation',
    python_callable=run_spark_aggregation,
    dag=dag,
)

transformation_task = PythonOperator(
    task_id='spark_transformations',
    python_callable=run_spark_transformations,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='summarize_results',
    python_callable=summarize_results,
    dag=dag,
)

# Task dependencies
[aggregation_task, transformation_task] >> summary_task
