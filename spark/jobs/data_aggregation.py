"""
PySpark Data Aggregation Job
Demonstrates distributed data processing with Spark
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, max as _max, min as _min, count,
    year, month, dayofmonth, hour, window, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import argparse


def create_spark_session(app_name="DataAggregation"):
    """Create Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.default.parallelism", "10") \
        .getOrCreate()


def read_sales_data(spark, input_path):
    """Read sales transaction data"""
    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("status", StringType(), True),
    ])

    # In production, read from database or data lake
    # For demo, create sample data
    df = spark.createDataFrame([
        (1, 101, 1, 1299.99, "USD", "2024-01-15 10:30:00", "completed"),
        (2, 102, 2, 29.99, "USD", "2024-01-15 11:00:00", "completed"),
        (3, 101, 3, 199.99, "USD", "2024-01-15 14:20:00", "completed"),
        (4, 103, 1, 1299.99, "USD", "2024-01-16 09:15:00", "completed"),
        (5, 102, 4, 499.99, "USD", "2024-01-16 16:45:00", "completed"),
    ], schema)

    return df


def aggregate_by_user(df):
    """Aggregate sales metrics by user"""
    user_agg = df.filter(col("status") == "completed") \
        .groupBy("user_id") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            _sum("amount").alias("total_spent"),
            avg("amount").alias("avg_transaction_value"),
            _max("amount").alias("max_transaction"),
            _min("amount").alias("min_transaction")
        ) \
        .orderBy(col("total_spent").desc())

    return user_agg


def aggregate_by_product(df):
    """Aggregate sales metrics by product"""
    product_agg = df.filter(col("status") == "completed") \
        .groupBy("product_id") \
        .agg(
            count("transaction_id").alias("total_sales"),
            _sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_price")
        ) \
        .orderBy(col("total_revenue").desc())

    return product_agg


def aggregate_by_time(df):
    """Aggregate sales metrics by time periods"""
    df_with_time = df.withColumn("year", year(col("transaction_date"))) \
        .withColumn("month", month(col("transaction_date"))) \
        .withColumn("day", dayofmonth(col("transaction_date"))) \
        .withColumn("hour", hour(col("transaction_date")))

    time_agg = df_with_time.filter(col("status") == "completed") \
        .groupBy("year", "month", "day") \
        .agg(
            count("transaction_id").alias("daily_transactions"),
            _sum("amount").alias("daily_revenue"),
            avg("amount").alias("avg_transaction_value")
        ) \
        .orderBy("year", "month", "day")

    return time_agg


def write_results(df, output_path, partition_by=None):
    """Write aggregated results"""
    writer = df.coalesce(1)

    if partition_by:
        writer = writer.write.partitionBy(partition_by)
    else:
        writer = writer.write

    writer.mode("overwrite") \
        .parquet(output_path)

    print(f"Results written to {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Spark Data Aggregation Job')
    parser.add_argument('--input', type=str, default='/opt/spark-data/raw',
                        help='Input data path')
    parser.add_argument('--output', type=str, default='/opt/spark-data/processed',
                        help='Output data path')

    args = parser.parse_args()

    # Create Spark session
    spark = create_spark_session()

    print("=" * 50)
    print("Starting Data Aggregation Job")
    print("=" * 50)

    # Read data
    print("\n1. Reading sales data...")
    sales_df = read_sales_data(spark, args.input)
    print(f"   Loaded {sales_df.count()} transactions")
    sales_df.show(5)

    # User aggregations
    print("\n2. Aggregating by user...")
    user_agg = aggregate_by_user(sales_df)
    user_agg.show()
    write_results(user_agg, f"{args.output}/user_aggregations")

    # Product aggregations
    print("\n3. Aggregating by product...")
    product_agg = aggregate_by_product(sales_df)
    product_agg.show()
    write_results(product_agg, f"{args.output}/product_aggregations")

    # Time-based aggregations
    print("\n4. Aggregating by time...")
    time_agg = aggregate_by_time(sales_df)
    time_agg.show()
    write_results(time_agg, f"{args.output}/time_aggregations",
                  partition_by=["year", "month"])

    print("\n" + "=" * 50)
    print("Data Aggregation Job Completed Successfully")
    print("=" * 50)

    spark.stop()


if __name__ == "__main__":
    main()
