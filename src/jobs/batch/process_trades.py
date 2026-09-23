import sys
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    coalesce,
    lit,
    try_to_timestamp,
    to_date,
)
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


# Read the job name supplied by AWS Glue
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"],
)


# Start Spark and AWS Glue
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session


# Start the Glue job
job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# Read the raw trades table from the Glue Data Catalog
raw_trades_df = (
    glue_context
    .create_dynamic_frame
    .from_catalog(
        database="trade_risk_raw_db",
        table_name="trades",
    )
    .toDF()
)
# Display basic information in the Glue logs
print("Raw trade count:", raw_trades_df.count())
raw_trades_df.printSchema()
raw_trades_df.show(5, truncate=False)
# Standardize text values
standardized_trades_df = (
    raw_trades_df
    .withColumn(
        "trade_type",
        upper(trim(col("trade_type"))),
    )
    .withColumn(
        "trade_currency",
        upper(trim(col("trade_currency"))),
    )
)
# Define the rules for a valid trade
valid_trade_condition = coalesce(
    col("trade_id").isNotNull()
    & col("account_id").isNotNull()
    & col("security_id").isNotNull()
    & col("counterparty_id").isNotNull()
    & col("trade_type").isin("BUY", "SELL")
    & (col("quantity") > 0)
    & (col("trade_price") > 0),
    lit(False),
)
# Separate valid and invalid records
valid_trades_df = standardized_trades_df.filter(
    valid_trade_condition
)

invalid_trades_df = standardized_trades_df.filter(
    ~valid_trade_condition
)
# Remove identical duplicate rows
unique_trades_df = valid_trades_df.dropDuplicates()
# Use UTC when interpreting and extracting dates
spark.conf.set("spark.sql.session.timeZone", "UTC")


# Convert the timestamp text and extract the trade date
dated_trades_df = (
    unique_trades_df
    .withColumn(
        "trade_timestamp",
        try_to_timestamp(col("trade_timestamp")),
    )
    .withColumn(
        "trade_date",
        to_date(col("trade_timestamp")),
    )
)
# Separate valid and invalid timestamps
clean_trades_df = dated_trades_df.filter(
    col("trade_timestamp").isNotNull()
)

invalid_timestamp_trades_df = dated_trades_df.filter(
    col("trade_timestamp").isNull()
)

# Check cleaning results
# Calculate each trade's value
clean_trades_df = clean_trades_df.withColumn(
    "trade_value",
    col("quantity") * col("trade_price"),
)

# Check cleaning results
print("Invalid trade count:", invalid_trades_df.count())
print("Valid trade count before deduplication:", valid_trades_df.count())
print("Unique trade count:", unique_trades_df.count())
print("Invalid timestamp count:", invalid_timestamp_trades_df.count())
print("Clean trade count:", clean_trades_df.count())

clean_trades_df.printSchema()
clean_trades_df.show(5, truncate=False)

# Save cleaned trades to S3, organized by trade date
(
    clean_trades_df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("trade_date")
    .parquet(
        "s3://cm-trade-risk-lake-adnan-6373/processed/trades_with_value/"
    )
)

print("Cleaned trades with trade value saved to S3.")

job.commit()