import sys
import re
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

# ----------------------------
# Glue boilerplate
# ----------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ----------------------------
# S3 client setup
# ----------------------------
s3 = boto3.client("s3")

# ----------------------------
# Configuration - You can extend this!
# ----------------------------
RAW_BUCKET = "salesforce-objects-rawzone"
CURATED_BUCKET = "curated-bc"
CURATED_PREFIX = "bc-curatedzone-sf"

# Map dataset names to columns to drop
COLUMNS_TO_DROP = {
    "salesforce-data": "No_of_demo_calls__c",
    "salesforce-data1": "Planned_DCR__c"
}

# ----------------------------
# Function to list all raw folders
# ----------------------------
def list_raw_folders(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    operation_parameters = {'Bucket': bucket, 'Prefix': prefix, 'Delimiter': '/'}
    page_iterator = paginator.paginate(**operation_parameters)

    folders = []
    for page in page_iterator:
        for content in page.get("CommonPrefixes", []):
            folders.append(content.get("Prefix"))
    return folders

# ----------------------------
# Function to extract date from folder path
# ----------------------------
def extract_date_from_path(path):
    match = re.search(r'(\d{4}-\d{2}-\d{2})T', path)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d")
    else:
        return datetime.today()  # fallback

# ----------------------------
# Processing logic
# ----------------------------
def process_and_write(source_path, column_to_drop, target_subfolder, process_date):
    print(f"📦 Processing: {source_path}")
    df = spark.read.parquet(f"s3://{RAW_BUCKET}/{source_path}")
    df_cleaned = df.drop(column_to_drop)

    year = process_date.year
    month = f"{process_date.month:02d}"
    day = f"{process_date.day:02d}"

    target_path = f"s3://{CURATED_BUCKET}/{CURATED_PREFIX}/{target_subfolder}/year={year}/month={month}/day={day}/"

    df_cleaned.write.mode("overwrite").parquet(target_path)
    print(f"✅ Written to: {target_path}")

# ----------------------------
# Main Dynamic Logic
# ----------------------------
for dataset, column in COLUMNS_TO_DROP.items():
    dataset_prefix = f"{dataset}/"
    folders = list_raw_folders(RAW_BUCKET, dataset_prefix)

    for folder in folders:
        process_date = extract_date_from_path(folder)
        cleaned_folder = f"{dataset}-cleaned"
        process_and_write(folder, column, cleaned_folder, process_date)

# ----------------------------
# Finalize Job
# ----------------------------
job.commit()
