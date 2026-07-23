from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp
import json

@dp.table(
    name="finguard.bronze.transactions",
    comment="Transactions raw stream data ingested by kafka"
)

def transactions_bronze() -> DataFrame:
    kafka_connection_json = dbutils.secrets.get(scope="finguard-scope", key="kafka_connection_details")
    kafka_connection = json.loads(kafka_connection_json)

    BOOTSTRAP_SERVERS = kafka_connection["BOOTSTRAP_SERVERS"]
    API_KEY = kafka_connection["API_KEY"]
    API_SECRET = kafka_connection["API_SECRET"]
    TOPIC_NAME = kafka_connection["TOPIC_NAME"]

    jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";'

    streaming_df = (
        spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers",BOOTSTRAP_SERVERS)
            .option("subscribe",TOPIC_NAME)
            .option("kafka.security.protocol","SASL_SSL")
            .option("kafka.sasl.mechanism","PLAIN")
            .option("kafka.sasl.jaas.config",jaas_config)
            .option("startingOffsets","earliest")
            .load()
        )
    
    parsed_streaming_df = streaming_df.select(
        col("key").cast("string"),
        col("value").cast("string"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("timestampType"),
        current_timestamp().alias("ingestion_timestamp")
        )
    
    return parsed_streaming_df
