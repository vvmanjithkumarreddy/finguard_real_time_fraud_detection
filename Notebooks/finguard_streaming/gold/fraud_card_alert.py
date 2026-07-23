from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import concat_ws,lit,col,current_timestamp

@dp.table(
    name="finguard.gold.fraud_card_alert",
    comment="Alert details where transactions has been performed with fraud credit cards"
)

def fraud_card_alert() -> DataFrame:
    transactions = spark.readStream.table("finguard.silver.transactions")
    fraud_watchlist = spark.readStream.table("finguard.silver.fraud_watchlist")
    customers = spark.read.table("finguard.silver.customers")

    transactions_with_watermark=transactions.withWatermark("transaction_timestamp","5 minutes")
    fraud_watchlist_with_watermark=fraud_watchlist.withWatermark("effective_from","5 minutes")

    fraud_detected = (
        transactions_with_watermark.join(
            fraud_watchlist_with_watermark, 
            transactions_with_watermark.card_number==fraud_watchlist_with_watermark.entity_id,
            "inner"
            )
        .join(
            customers,
            transactions_with_watermark.customer_id==customers.customer_id,
            "left"
        )
        .select(
            #Alert identification
            concat_ws("-",lit("FRAUD"),col("transaction_id"),col("watchlist_id")).alias("alert_id"),
            lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
            current_timestamp().alias("alert_timestamp"),

            #Transaction details
            transactions.transaction_id,
            transactions.customer_id,
            customers.email.alias("customer_email"),
            concat_ws(" ",col("first_name"),col("last_name")).alias("customer_name"),
            transactions.amount.alias("transaction_amount"),
            customers.transaction_limit,
            transactions.currency,
            transactions.merchant_name,
            transactions.merchant_category,
            transactions.transaction_type,
            transactions.payment_channel,
            transactions.city.alias("transaction_city"),
            transactions.country.alias("transaction_country"),
            transactions.is_international,
            transactions.transaction_timestamp,
            transactions.status.alias("transaction_status"),

            #Fraud Watchlist details
            col("watchlist_id"),
            col("watch_type"),
            col("risk_level"),
            col("action"),
            col("reason_code"),
            col("reason_description"),
            col("effective_from").alias("watchlist_effective_from"),
            col("reported_by"),
            col("reported_source"),
            fraud_watchlist_with_watermark.city.alias("watchlist_city"),
            fraud_watchlist_with_watermark.country.alias("watchlist_country")
        )
    )

    return fraud_detected








