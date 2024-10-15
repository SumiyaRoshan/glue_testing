import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, max as spark_max, concat, lit, to_timestamp
from datetime import datetime, timedelta
from pyspark.sql.functions import col, concat, lpad

# Initialize contexts and session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path', 'load_type'])
input_path = args['input_path']
output_path = args['output_path']
load_type = args['load_type']

def combine_date_time(df):
    return df.withColumn(
        "entry_timestamp_str",
        concat(
            col("ERDAT"),
            lpad(col("ERZET"), 6, "0")  # Ensure ERZET is always 6 digits
        )
    )

def full_load():
    # Read CSV from S3
    df = spark.read.csv(input_path, header=True)
    
    # Combine 'Date of Entry' and 'Time of Entry' into a single timestamp column
    df = combine_date_time(df)
    
    # Write to S3 (overwrite mode for full load)
    df.coalesce(1).write.mode("overwrite").parquet(output_path)

def cdc_load():
    # Read the last watermark value
    try:
        last_watermark = spark.read.parquet(output_path).agg(spark_max("entry_timestamp_str")).collect()[0][0]
    except:
        # If no data exists, use a default start date (e.g., yesterday)
        last_watermark = datetime.now() - timedelta(days=1)
    print(last_watermark)
    # Read CSV from S3
    df = spark.read.csv(input_path, header=True)
    
    # Combine 'Date of Entry' and 'Time of Entry' into a single timestamp column
    df = combine_date_time(df)
    df.show()
    # Filter the data based on the last watermark
    df = df.filter(col("entry_timestamp_str") > last_watermark)

        
    df.write.mode("append").parquet(output_path)

# Main execution
if load_type.upper() == "FULL LOAD":
    print(f"Executing Full Load for input {input_path}")
    full_load()
elif load_type.upper() == "CDC LOAD":
    print(f"Executing CDC Load for input {input_path}")
    cdc_load()
# else:
#     raise ValueError(f"Invalid load_type: {load_type}. Must be either 'FULL LOAD' or 'CDC LOAD'.")

# Update the job bookmark
job.commit()