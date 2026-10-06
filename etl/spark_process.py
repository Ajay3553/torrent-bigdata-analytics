import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, when, current_timestamp, round

def main():
    print("Starting Spark ETL Processing Job...")
    
    spark = SparkSession.builder \
        .appName("Torrent-ETL-Processing") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    catalog_path = "warehouse/rarbg_catalog.csv"
    if not os.path.exists(catalog_path):
        print(f"Error: {catalog_path} not found.")
        return

    print(f"Reading raw catalog data from {catalog_path}...")
    df = spark.read.option("header", "true").csv(catalog_path)

    print("Cleaning torrent titles and extracting resolution quality...")
    cleaned_df = df.withColumn("clean_title", lower(col("title"))) \
                   .withColumn("clean_title", regexp_replace(col("clean_title"), r"[\._\-\(\)]", " ")) \
                   .withColumn("quality", 
                       when(col("title").rlike("(?i)2160p|4k"), "4K")
                       .when(col("title").rlike("(?i)1080p"), "1080p")
                       .when(col("title").rlike("(?i)720p"), "720p")
                       .otherwise("SD")
                   )

    processed_df = cleaned_df.withColumn("size_gb", round(col("size_bytes") / (1024 * 1024 * 1024), 2)) \
                             .withColumn("processed_at", current_timestamp())

    print("\n--- Sample Processed Metadata Output ---")
    processed_df.select("infohash", "clean_title", "category", "quality", "size_gb").show(5, truncate=False)

    # Convert to Pandas to write locally without winutils error
    output_dir = "warehouse/processed_catalog.csv"
    print(f"Saving processed catalog to {output_dir}...")
    pdf = processed_df.toPandas()
    pdf.to_csv(output_dir, index=False)

    print("\nSpark ETL Processing Job Completed Successfully!")
    spark.stop()

if __name__ == "__main__":
    main()