from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, upper, current_timestamp, to_timestamp
import json

@dp.table(
    name="finguard.silver.fraud_watchlist",
    comment="Cleaned Fraud Watchlist"
)

def fraud_watchlist_silver() -> DataFrame:
    bronze_df = (
        spark.readStream
        .table("finguard.bronze.fraud_watchlist")
        )
    
    cleaned_df = bronze_df.select(
        col("watchlist_id"),
        col("watch_type"),
        upper(col("entity_id")).alias("entity_id"),
        upper(col("risk_level")).alias("risk_level"),
        upper(col("action")).alias("action"),
        col("reason_code"),
        col("reason_description"),
        col("status"),
        to_timestamp(col("effective_from"), "dd-MMM-yyyy HH:mm:ss").alias("effective_from"),
        col("reported_by"),
        col("reported_source"),
        col("country"),
        col("city"),
        col("_rescued_data"),
        col("source_file_path"),
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        current_timestamp().alias("silver_ingestion_timestamp")
        )
    
    return cleaned_df
