import os
import sys
import time
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, to_timestamp, row_number
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, MapType
from pyspark.sql.window import Window
import boto3
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def initialize_spark(app_name="DynamoDBCDCToIceberg"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    logger.info(f"Spark session created - Version: {spark.version}")
    return spark


def get_s3_client():
    """Initialize S3 client with AWS credentials"""
    return boto3.client('s3')


def parse_s3_path(s3_uri):
    """Parse S3 URI into bucket and key"""
    parsed = urlparse(s3_uri)
    return parsed.netloc, parsed.path.lstrip('/')


def create_pending_manifest(s3_client, bucket_name, raw_prefix, pending_prefix, max_files=10000):
    """
    Create a manifest file listing all JSON.GZ files to process
    Returns: S3 path to manifest file or None if no files found
    """
    logger.info(f"Scanning for files in s3://{bucket_name}/{raw_prefix}")

    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=raw_prefix)

    files = []
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            # Only process .json.gz files
            if key.endswith('.json.gz'):
                files.append(f"s3a://{bucket_name}/{key}")
                if len(files) >= max_files:
                    break
        if len(files) >= max_files:
            break

    if not files:
        logger.warning("No JSON.GZ files found to process")
        return None

    # Create manifest file
    manifest_content = '\n'.join(files)
    unix_ts = int(time.time())
    manifest_key = f"{pending_prefix}manifest_{unix_ts}.pending"

    s3_client.put_object(
        Bucket=bucket_name,
        Key=manifest_key,
        Body=manifest_content
    )

    logger.info(f"Created manifest with {len(files)} files: s3://{bucket_name}/{manifest_key}")
    return f"s3a://{bucket_name}/{manifest_key}"


def read_json_from_manifest(spark, manifest_path):
    """
    Read manifest file and process all JSON.GZ files
    Returns: Spark DataFrame with CDC events
    """
    logger.info(f"Reading manifest from: {manifest_path}")

    # Read manifest to get list of files
    manifest_df = spark.read.text(manifest_path)
    file_paths = [row[0] for row in manifest_df.collect()]

    logger.info(f"Processing {len(file_paths)} JSON.GZ files")

    # Read all JSON.GZ files (Spark handles gzip automatically)
    df = spark.read.json(file_paths)
    logger.info(df.show())
    df.show()
    print("\n")
    logger.info(df.printSchema())
    print(df.printSchema())
    print("\n")

    logger.info(f"Loaded {df.count()} CDC events")
    return df


def create_iceberg_table_if_not_exists(spark, namespace="dynamodb", table_name="user_profiles"):
    """
    Create Iceberg table with VARIANT type for properties field
    Uses Iceberg format version 3 to support VARIANT
    """
    full_table_name = f"s3tablesbucket.{namespace}.{table_name}"

    # Create namespace
    spark.sql(f"""
        CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.{namespace}
    """)
    logger.info(f"Namespace s3tablesbucket.{namespace} created or confirmed")

    # Create table with VARIANT for properties
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            user_id STRING,
            created_at BIGINT,
            email STRING,
            username STRING,
            status STRING,
            plan_type STRING,
            registration_date STRING,
            properties VARIANT,
            event_id STRING,
            event_name STRING,
            event_timestamp TIMESTAMP,
            processed_timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (status)
        TBLPROPERTIES (
            'format-version' = '3'
        )
    """)

    logger.info(f"Table {full_table_name} created or confirmed with VARIANT support")
    return full_table_name


def transform_cdc_to_user_profile(spark, cdc_df):
    """
    Transform CDC events to user profile records
    Extracts fields from new_image and converts properties to JSON string for VARIANT
    """
    logger.info("Transforming CDC events to user profile records")

    # Filter only INSERT and MODIFY events
    filtered_df = cdc_df.filter(col("event_name").isin(["INSERT", "MODIFY"]))

    # Create temp view for SQL transformation
    filtered_df.createOrReplaceTempView("cdc_events")

    # Extract fields and prepare for Iceberg
    transformed_df = spark.sql("""
                               SELECT new_image.user_id                                AS user_id,
                                      CAST(new_image.created_at AS BIGINT)             AS created_at,
                                      new_image.email                                  AS email,
                                      new_image.username                               AS username,
                                      new_image.status                                 AS status,
                                      new_image.plan_type                              AS plan_type,
                                      new_image.registration_date                      AS registration_date,
                                      parse_json(to_json(new_image.properties))        AS properties,
                                      event_id,
                                      event_name,
                                      CAST(approximate_creation_datetime AS TIMESTAMP) AS event_timestamp,
                                      CAST(processed_timestamp AS TIMESTAMP)           AS processed_timestamp,
                                      ROW_NUMBER()                                        OVER (
                PARTITION BY new_image.user_id, new_image.created_at 
                ORDER BY processed_timestamp DESC
            ) AS row_num
                               FROM cdc_events
                               WHERE new_image IS NOT NULL
                               """)

    # Keep only the latest event per user_id + created_at
    deduplicated_df = transformed_df.filter(col("row_num") == 1).drop("row_num")

    record_count = deduplicated_df.count()
    logger.info(f"Transformed and deduplicated to {record_count} user profile records")

    return deduplicated_df


def merge_into_iceberg_table(spark, source_df, table_name):
    """
    Merge data into Iceberg table using MERGE INTO
    Primary key: user_id + created_at
    """
    logger.info(f"Starting merge into {table_name}")

    # Create temp view
    temp_view = "staging_user_profiles"
    source_df.createOrReplaceTempView(temp_view)

    # Perform merge
    merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING {temp_view} AS source
        ON target.user_id = source.user_id 
           AND target.created_at = source.created_at
        WHEN MATCHED THEN 
            UPDATE SET *
        WHEN NOT MATCHED THEN 
            INSERT *
    """

    spark.sql(merge_sql)
    spark.catalog.dropTempView(temp_view)

    logger.info("Merge completed successfully")


def archive_processed_files(s3_client, bucket_name, manifest_path, archived_prefix, error_prefix):
    """
    Archive processed JSON.GZ files to archived folder
    Move manifest to archived as well
    """
    logger.info("Starting archival process")

    _, manifest_key = parse_s3_path(manifest_path)

    try:
        # Read manifest
        response = s3_client.get_object(Bucket=bucket_name, Key=manifest_key)
        content = response['Body'].read().decode('utf-8')
        file_paths = [path.strip() for path in content.split('\n') if path.strip()]

        archived_count = 0
        for file_path in file_paths:
            _, old_key = parse_s3_path(file_path)

            # Preserve directory structure in archived folder
            relative_path = old_key.split('/', 1)[1] if '/' in old_key else old_key
            new_key = f"{archived_prefix}{relative_path}"

            try:
                # Copy to archived
                s3_client.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': old_key},
                    Key=new_key
                )

                # Delete original
                s3_client.delete_object(Bucket=bucket_name, Key=old_key)
                archived_count += 1

            except Exception as e:
                logger.error(f"Error archiving {old_key}: {str(e)}")
                continue

        # Delete manifest
        s3_client.delete_object(Bucket=bucket_name, Key=manifest_key)

        logger.info(f"Successfully archived {archived_count}/{len(file_paths)} files and deleted manifest")

    except Exception as e:
        logger.error(f"Error during archival: {str(e)}")
        # Move manifest to error folder
        try:
            error_key = f"{error_prefix}{os.path.basename(manifest_key)}"
            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': manifest_key},
                Key=error_key
            )
            logger.warning(f"Moved problematic manifest to: {error_key}")
        except Exception as e2:
            logger.error(f"Failed to move manifest to error folder: {str(e2)}")


def main():
    """Main ETL pipeline"""
    # Configuration
    bucket_name = os.getenv('S3_BUCKET', 'dev-user-profile-cdc-371580379745')
    raw_prefix = os.getenv('RAW_PREFIX', 'user_profiles/')
    pending_prefix = os.getenv('PENDING_PREFIX', 'pending/')
    archived_prefix = os.getenv('ARCHIVED_PREFIX', 'archived/')
    error_prefix = os.getenv('ERROR_PREFIX', 'error/')
    namespace = os.getenv('ICEBERG_NAMESPACE', 'dynamodb')
    table_name = os.getenv('ICEBERG_TABLE', 'user_profiles')
    max_files = int(os.getenv('MAX_FILES', '10000'))

    logger.info("=" * 80)
    logger.info("Starting DynamoDB CDC to Iceberg ETL Pipeline")
    logger.info(f"Configuration: bucket={bucket_name}, namespace={namespace}, table={table_name}")
    logger.info("=" * 80)

    # Initialize clients
    spark = initialize_spark()
    s3_client = get_s3_client()

    try:
        # Step 1: Create pending manifest
        logger.info("STEP 1: Creating pending manifest")
        manifest_path = create_pending_manifest(
            s3_client=s3_client,
            bucket_name=bucket_name,
            raw_prefix=raw_prefix,
            pending_prefix=pending_prefix,
            max_files=max_files
        )

        if manifest_path is None:
            logger.info("No files to process. Exiting.")
            return

        # Step 2: Read JSON data from manifest
        logger.info("STEP 2: Reading JSON.GZ files from S3")
        cdc_df = read_json_from_manifest(spark, manifest_path)

        # Step 3: Create Iceberg table if not exists
        logger.info("STEP 3: Creating/confirming Iceberg table")
        full_table_name = create_iceberg_table_if_not_exists(
            spark=spark,
            namespace=namespace,
            table_name=table_name
        )

        # Step 4: Transform CDC events to user profiles
        logger.info("STEP 4: Transforming CDC events")
        user_profile_df = transform_cdc_to_user_profile(spark, cdc_df)

        # Show sample data
        logger.info("Sample transformed records:")
        user_profile_df.select("user_id", "email", "status", "event_name").show(5, truncate=False)

        # Step 5: Merge into Iceberg table
        logger.info("STEP 5: Merging into Iceberg table")
        merge_into_iceberg_table(
            spark=spark,
            source_df=user_profile_df,
            table_name=full_table_name
        )

        # Step 6: Verify data in Iceberg
        logger.info("STEP 6: Verifying data in Iceberg")
        result_df = spark.sql(f"""
            SELECT 
                user_id, 
                email, 
                status,
                variant_get(properties, '$.profile.first_name', 'string') AS first_name,
                variant_get(properties, '$.subscription.plan', 'string') AS subscription_plan
            FROM {full_table_name}
            LIMIT 10
        """)
        result_df.show(truncate=False)

        # Get total count
        total_count = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}").collect()[0]['count']
        logger.info(f"Total records in Iceberg table: {total_count}")

        # Step 7: Archive processed files
        logger.info("STEP 7: Archiving processed files")
        archive_processed_files(
            s3_client=s3_client,
            bucket_name=bucket_name,
            manifest_path=manifest_path,
            archived_prefix=archived_prefix,
            error_prefix=error_prefix
        )

        logger.info("=" * 80)
        logger.info("ETL Pipeline completed successfully!")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Error in ETL pipeline: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

        # Try to move manifest to error folder
        if manifest_path:
            try:
                _, manifest_key = parse_s3_path(manifest_path)
                error_key = f"{error_prefix}{os.path.basename(manifest_key)}"
                s3_client.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': manifest_key},
                    Key=error_key
                )
                logger.warning(f"Moved failed manifest to error folder: {error_key}")
            except Exception as e2:
                logger.error(f"Failed to move manifest to error: {str(e2)}")

        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
