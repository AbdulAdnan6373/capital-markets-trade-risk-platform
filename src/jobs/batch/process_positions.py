import sys

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    coalesce,
    lit,
    to_date,
    abs as spark_abs,
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


# Read raw positions from the Glue Data Catalog
raw_positions_df = (
    glue_context
    .create_dynamic_frame
    .from_catalog(
        database="trade_risk_raw_db",
        table_name="positions",
    )
    .toDF()
)


# Display basic information in the Glue logs
print("Raw position count:", raw_positions_df.count())
raw_positions_df.printSchema()
raw_positions_df.show(5, truncate=False)
# Standardize position type and date
standardized_positions_df = (
    raw_positions_df
    .withColumn(
        "position_type",
        upper(trim(col("position_type"))),
    )
    .withColumn(
        "position_date",
        to_date(col("position_date")),
    )
)


# Define the rules for a valid position
valid_position_condition = coalesce(
    col("position_id").isNotNull()
    & col("account_id").isNotNull()
    & col("security_id").isNotNull()
    & col("position_date").isNotNull()
    & col("position_type").isin("LONG", "SHORT")
    & (col("average_cost") > 0)
    & (
        ((col("position_type") == "LONG") & (col("position_quantity") > 0))
        | ((col("position_type") == "SHORT") & (col("position_quantity") < 0))
    ),
    lit(False),
)


# Separate valid and invalid positions
valid_positions_df = standardized_positions_df.filter(
    valid_position_condition
)

invalid_positions_df = standardized_positions_df.filter(
    ~valid_position_condition
)


# Remove duplicate position IDs
clean_positions_df = valid_positions_df.dropDuplicates(
    ["position_id"]
)
# Calculate the cost-basis value of each position
enriched_positions_df = clean_positions_df.withColumn(
    "cost_basis_value",
    spark_abs(col("position_quantity")) * col("average_cost"),
)


# Check the processing results
print("Invalid position count:", invalid_positions_df.count())
print("Valid position count:", valid_positions_df.count())
print("Clean position count:", clean_positions_df.count())
print("Enriched position count:", enriched_positions_df.count())

enriched_positions_df.printSchema()
enriched_positions_df.show(5, truncate=False)


# Save processed positions to S3 by position date
(
    enriched_positions_df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("position_date")
    .parquet(
        "s3://cm-trade-risk-lake-adnan-6373/processed/positions/"
    )
)

print("Processed positions saved to S3.")


# Mark the Glue job as completed
job.commit()