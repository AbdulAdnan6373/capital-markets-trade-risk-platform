import sys

from pyspark.sql.functions import (
    col,
    coalesce,
    lit,
    row_number,
    to_date,
    try_to_timestamp,
)
from pyspark.sql.window import Window

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


# Use UTC when interpreting timestamps
spark.conf.set("spark.sql.session.timeZone", "UTC")


# Read raw market prices from the Glue Data Catalog
raw_prices_df = (
    glue_context
    .create_dynamic_frame
    .from_catalog(
        database="trade_risk_raw_db",
        table_name="market_prices",
    )
    .toDF()
)


print("Raw price count:", raw_prices_df.count())
raw_prices_df.printSchema()
raw_prices_df.show(5, truncate=False)


# Convert the timestamp text and create the price date
standardized_prices_df = (
    raw_prices_df
    .withColumn(
        "price_timestamp",
        try_to_timestamp(col("price_timestamp")),
    )
    .withColumn(
        "price_date",
        to_date(col("price_timestamp")),
    )
)


# Define the rules for a valid market-price record
valid_price_condition = coalesce(
    col("price_id").isNotNull()
    & col("security_id").isNotNull()
    & col("price_timestamp").isNotNull()
    & (col("open_price") > 0)
    & (col("high_price") > 0)
    & (col("low_price") > 0)
    & (col("close_price") > 0)
    & (col("trading_volume") >= 0)
    & (col("high_price") >= col("open_price"))
    & (col("high_price") >= col("close_price"))
    & (col("high_price") >= col("low_price"))
    & (col("low_price") <= col("open_price"))
    & (col("low_price") <= col("close_price")),
    lit(False),
)


# Separate valid and invalid records
valid_prices_df = standardized_prices_df.filter(
    valid_price_condition
)

invalid_prices_df = standardized_prices_df.filter(
    ~valid_price_condition
)


# Remove duplicate price records
unique_prices_df = valid_prices_df.dropDuplicates(
    ["price_id"]
)


# Rank prices from newest to oldest for each security
latest_price_window = (
    Window
    .partitionBy("security_id")
    .orderBy(
        col("price_timestamp").desc(),
        col("price_id").desc(),
    )
)


ranked_prices_df = unique_prices_df.withColumn(
    "price_rank",
    row_number().over(latest_price_window),
)


# Keep only the newest price for each security
latest_prices_df = (
    ranked_prices_df
    .filter(col("price_rank") == 1)
    .drop("price_rank")
)


# Check processing results
print("Invalid price count:", invalid_prices_df.count())
print("Valid price count:", valid_prices_df.count())
print("Unique price count:", unique_prices_df.count())
print("Latest price count:", latest_prices_df.count())

latest_prices_df.printSchema()
latest_prices_df.show(5, truncate=False)


# Save the latest market prices to S3
(
    latest_prices_df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("price_date")
    .parquet(
        "s3://cm-trade-risk-lake-adnan-6373/processed/market_prices_latest/"
    )
)


print("Latest market prices saved to S3.")

job.commit()