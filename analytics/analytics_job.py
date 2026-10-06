import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, desc, avg, sum as _sum

def main():
    print("Starting Analytics & Swarm Aggregation Job...")

    spark = SparkSession.builder \
        .appName("Torrent-Swarm-Analytics") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    catalog_path = "warehouse/processed_catalog.csv"
    if not os.path.exists(catalog_path):
        print(f"Error: {catalog_path} not found. Run ETL phase first.")
        return

    print("Loading processed torrent catalog...")
    catalog_df = spark.read.option("header", "true").csv(catalog_path)

    # 1. Category Distribution & Storage Metrics
    print("\n==========================================")
    print(" 1. CATEGORY DISTRIBUTION & STORAGE SUMMARY ")
    print("==========================================")
    category_summary = catalog_df.groupBy("category") \
        .agg(
            round(_sum("size_gb"), 2).alias("total_size_gb"),
            round(avg("size_gb"), 2).alias("avg_size_gb")
        ) \
        .orderBy(desc("total_size_gb"))
    
    category_summary.show(truncate=False)

    # 2. Quality / Resolution Breakdown
    print("\n==========================================")
    print(" 2. QUALITY / RESOLUTION BREAKDOWN ")
    print("==========================================")
    quality_summary = catalog_df.groupBy("quality").count().orderBy(desc("count"))
    quality_summary.show(truncate=False)

    # Save summary metrics locally using Pandas to avoid Windows lock issues
    os.makedirs("warehouse/analytics", exist_ok=True)
    summary_output = "warehouse/analytics/category_metrics.csv"
    print(f"Saving category metrics to {summary_output}...")
    
    pdf = category_summary.toPandas()
    pdf.to_csv(summary_output, index=False)

    print("\nAnalytics Job Completed Successfully!")
    spark.stop()

if __name__ == "__main__":
    main()