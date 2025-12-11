"""
PySpark Data Transformation Job
Demonstrates complex transformations, joins, and window functions
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, when, coalesce, concat, concat_ws,
    row_number, rank, dense_rank, lag, lead,
    sum as _sum, avg, count, datediff, current_date,
    to_date, date_format, explode, split
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import argparse


def create_spark_session(app_name="DataTransformation"):
    """Create Spark session with optimized configs"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()


def create_sample_data(spark):
    """Create sample datasets for demonstration"""

    # Users data
    users_schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("username", StringType(), True),
        StructField("email", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("country", StringType(), True),
    ])

    users_data = [
        (101, "alice_smith", "alice@example.com", "2023-01-15", "USA"),
        (102, "bob_jones", "bob@example.com", "2023-02-20", "Canada"),
        (103, "charlie_brown", "charlie@example.com", "2023-03-10", "UK"),
        (104, "diana_prince", "diana@example.com", "2023-04-05", "USA"),
    ]

    users_df = spark.createDataFrame(users_data, users_schema)

    # Transactions data
    transactions_schema = StructType([
        StructField("transaction_id", IntegerType(), False),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("status", StringType(), True),
    ])

    transactions_data = [
        (1, 101, 1, 1299.99, "2024-01-15", "completed"),
        (2, 102, 2, 29.99, "2024-01-15", "completed"),
        (3, 101, 3, 199.99, "2024-01-16", "completed"),
        (4, 103, 1, 1299.99, "2024-01-17", "completed"),
        (5, 102, 4, 499.99, "2024-01-18", "completed"),
        (6, 101, 2, 29.99, "2024-01-19", "completed"),
        (7, 104, 5, 49.99, "2024-01-20", "failed"),
        (8, 103, 3, 199.99, "2024-01-21", "completed"),
    ]

    transactions_df = spark.createDataFrame(transactions_data, transactions_schema)

    # Products data
    products_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
    ])

    products_data = [
        (1, "Laptop Pro", "Electronics", 1299.99),
        (2, "Wireless Mouse", "Electronics", 29.99),
        (3, "Desk Chair", "Furniture", 199.99),
        (4, "Standing Desk", "Furniture", 499.99),
        (5, "USB-C Hub", "Electronics", 49.99),
    ]

    products_df = spark.createDataFrame(products_data, products_schema)

    return users_df, transactions_df, products_df


def enrich_transactions(transactions_df, users_df, products_df):
    """Join transactions with users and products to create enriched dataset"""

    # Convert date strings to date type
    transactions_df = transactions_df.withColumn(
        "transaction_date",
        to_date(col("transaction_date"))
    )

    users_df = users_df.withColumn(
        "registration_date",
        to_date(col("registration_date"))
    )

    # Perform joins
    enriched_df = transactions_df \
        .join(users_df, "user_id", "left") \
        .join(products_df, "product_id", "left")

    # Add calculated columns
    enriched_df = enriched_df \
        .withColumn("user_full_name",
                    concat(col("username"), lit(" ("), col("country"), lit(")"))) \
        .withColumn("days_since_registration",
                    datediff(col("transaction_date"), col("registration_date"))) \
        .withColumn("is_electronics",
                    when(col("category") == "Electronics", lit(True)).otherwise(lit(False))) \
        .withColumn("transaction_year_month",
                    date_format(col("transaction_date"), "yyyy-MM"))

    return enriched_df


def apply_window_functions(df):
    """Apply window functions for advanced analytics"""

    # Window specifications
    user_window = Window.partitionBy("user_id").orderBy("transaction_date")
    category_window = Window.partitionBy("category").orderBy(col("amount").desc())
    monthly_window = Window.partitionBy("transaction_year_month").orderBy(col("amount").desc())

    # Apply window functions
    df_with_windows = df \
        .withColumn("user_transaction_number",
                    row_number().over(user_window)) \
        .withColumn("user_running_total",
                    _sum("amount").over(user_window)) \
        .withColumn("previous_transaction_amount",
                    lag("amount", 1).over(user_window)) \
        .withColumn("next_transaction_amount",
                    lead("amount", 1).over(user_window)) \
        .withColumn("category_sales_rank",
                    rank().over(category_window)) \
        .withColumn("monthly_transaction_rank",
                    dense_rank().over(monthly_window))

    return df_with_windows


def calculate_user_segments(df):
    """Segment users based on spending behavior"""

    # Calculate user-level metrics
    user_metrics = df.filter(col("status") == "completed") \
        .groupBy("user_id", "username", "country") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            _sum("amount").alias("total_spent"),
            avg("amount").alias("avg_transaction"),
            _sum(when(col("is_electronics"), 1).otherwise(0)).alias("electronics_purchases")
        )

    # Apply segmentation logic
    user_segments = user_metrics \
        .withColumn("spending_segment",
                    when(col("total_spent") >= 1000, "High Value")
                    .when(col("total_spent") >= 500, "Medium Value")
                    .otherwise("Low Value")) \
        .withColumn("purchase_frequency",
                    when(col("total_transactions") >= 5, "Frequent")
                    .when(col("total_transactions") >= 3, "Regular")
                    .otherwise("Occasional")) \
        .withColumn("product_preference",
                    when(col("electronics_purchases") / col("total_transactions") >= 0.7, "Electronics Enthusiast")
                    .when(col("electronics_purchases") / col("total_transactions") >= 0.3, "Mixed Buyer")
                    .otherwise("Furniture Focused"))

    return user_segments


def write_results(df, output_path, format="parquet", mode="overwrite"):
    """Write transformed data to output"""
    df.coalesce(1) \
        .write \
        .format(format) \
        .mode(mode) \
        .save(output_path)

    print(f"Results written to {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Spark Data Transformation Job')
    parser.add_argument('--output', type=str, default='/opt/spark-data/processed',
                        help='Output data path')

    args = parser.parse_args()

    # Create Spark session
    spark = create_spark_session()

    print("=" * 60)
    print("Starting Data Transformation Job")
    print("=" * 60)

    # Create sample data
    print("\n1. Creating sample datasets...")
    users_df, transactions_df, products_df = create_sample_data(spark)
    print(f"   Users: {users_df.count()}")
    print(f"   Transactions: {transactions_df.count()}")
    print(f"   Products: {products_df.count()}")

    # Enrich transactions
    print("\n2. Enriching transactions with user and product data...")
    enriched_df = enrich_transactions(transactions_df, users_df, products_df)
    enriched_df.show(5, truncate=False)
    write_results(enriched_df, f"{args.output}/enriched_transactions")

    # Apply window functions
    print("\n3. Applying window functions...")
    windowed_df = apply_window_functions(enriched_df)
    windowed_df.select(
        "user_id", "transaction_date", "amount",
        "user_transaction_number", "user_running_total",
        "category_sales_rank"
    ).show(10)
    write_results(windowed_df, f"{args.output}/windowed_transactions")

    # Calculate user segments
    print("\n4. Calculating user segments...")
    segments_df = calculate_user_segments(enriched_df)
    segments_df.show(truncate=False)
    write_results(segments_df, f"{args.output}/user_segments")

    # Summary statistics
    print("\n5. Summary Statistics:")
    print("-" * 60)
    enriched_df.groupBy("country", "category") \
        .agg(
            count("*").alias("transactions"),
            _sum("amount").alias("total_revenue")
        ) \
        .orderBy("country", col("total_revenue").desc()) \
        .show()

    print("\n" + "=" * 60)
    print("Data Transformation Job Completed Successfully")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
