import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# Create a GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())

# Get the AWS Glue job parameters
args = getResolvedOptions(sys.argv, ['CSV_TO_PARQUET'])

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Read the CSV files
df = glueContext.create_dynamic_frame.from_catalog(
    database = "newcovid19vijay",
    table_name = "states_daily"
)

# Convert the CSV data to Parquet format
df.toDF().write.parquet("s3://covid19vijay/c9c3d9c463f94e0a669f092b35273668/d36f1d52b7c8d06790db13cdd9340899/covid-19-testing-data/parquet-data/")

# End the Glue job
glueContext.end()
