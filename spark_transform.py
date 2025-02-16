from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, avg, sum, count
import sys

def main(input_path, output_path):
    # Initialize Spark session
    spark = SparkSession.builder.appName("NYC Taxi Processing").getOrCreate()
    
    # Load raw data
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Convert datetime columns
    df = df.withColumn("year", year(col("tpep_pickup_datetime")))
    df = df.withColumn("month", month(col("tpep_pickup_datetime")))
    
    # Aggregation: trips, avg distance, total revenue
    aggregated_df = df.groupBy("year", "month", "PULocationID", "DOLocationID").agg(
        count("VendorID").alias("trip_count"),
        avg("Trip_distance").alias("avg_distance"),
        sum("Total_amount").alias("total_revenue")
    )
    
    # Write to Parquet with partitioning
    aggregated_df.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)
    
    spark.stop()

if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)
