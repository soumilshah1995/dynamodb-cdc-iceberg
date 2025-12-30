# DynamoDB CDC to Iceberg Pipeline

Real-time pipeline capturing DynamoDB changes via Lambda, storing in S3, and processing with Spark to Iceberg tables with VARIANT support for nested JSON.

## Architecture

![Image](https://github.com/user-attachments/assets/b7195647-9cb1-4740-843d-c1ffb435016d)


```
DynamoDB (Streams) → Lambda (CDC) → S3 (JSON.GZ) → Spark (EMR) → Iceberg v3 (VARIANT)
```

**Flow**: DynamoDB streams trigger Lambda → Lambda parses & compresses to S3 → Spark reads files → MERGE into Iceberg → Archive processed files

---

## Quick Start

### 1. Deploy Infrastructure

```bash
# Install Serverless Framework
npm install -g serverless@^3

# Deploy stack
npx serverless deploy --stage dev --region us-west-2

# Insert sample data
python sample_data.py
```

### 2. Setup EMR & S3 Tables

```bash
# Create S3 Tables bucket
aws s3tables create-table-bucket --name soumil-demo --region us-west-2

# Upload Spark JARs
aws s3 cp iceberg-spark-runtime-4.0_2.13-1.10.0.jar s3://your-bucket/jars/
aws s3 cp s3-tables-catalog-for-iceberg-0.1.8.jar s3://your-bucket/jars/
aws s3 cp hadoop-aws-3.3.4.jar s3://your-bucket/jars/
aws s3 cp aws-java-sdk-bundle-1.12.661.jar s3://your-bucket/jars/
aws s3 cp awssdk-bundle-2.29.38.jar s3://your-bucket/jars/
aws s3 cp caffeine-3.1.8.jar s3://your-bucket/jars/
aws s3 cp commons-configuration2-2.11.0.jar s3://your-bucket/jars/

# Create EMR Serverless application
aws emr-serverless create-application \
  --release-label emr-8.0.0 \
  --type SPARK \
  --name "DynamoDB-CDC-Pipeline"
```

### 3. Submit Spark Job

```bash
# Configure environment
export BUCKET="soumil-demo-spark"
export IAM_ROLE_ARN="arn:aws:iam::371580379745:role/service-role/AmazonEMR-ExecutionRole-1767048605200"
export APPLICATION_ID="00g1s0c2l87nu80l"
export REGION="us-west-2"
export ACCOUNT_ID="371580379745"

# Define Spark parameters
SPARK_PARAMS="--jars s3://$BUCKET/jars/iceberg-spark-runtime-4.0_2.13-1.10.0.jar,\
s3://$BUCKET/jars/s3-tables-catalog-for-iceberg-0.1.8.jar,\
s3://$BUCKET/jars/hadoop-aws-3.3.4.jar,\
s3://$BUCKET/jars/aws-java-sdk-bundle-1.12.661.jar,\
s3://$BUCKET/jars/awssdk-bundle-2.29.38.jar,\
s3://$BUCKET/jars/caffeine-3.1.8.jar,\
s3://$BUCKET/jars/commons-configuration2-2.11.0.jar \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog \
--conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:$REGION:$ACCOUNT_ID:bucket/soumil-demo \
--conf spark.sql.catalog.s3tablesbucket.client.region=$REGION \
--conf spark.sql.catalog.s3tablesbucket.format-version=3"

# Upload job script
aws s3 cp ingest.py s3://$BUCKET/jobs/jobv2.py

# Submit job
aws emr-serverless start-job-run \
  --application-id "$APPLICATION_ID" \
  --name "DynamoDB-CDC-Iceberg-Pipeline" \
  --execution-role-arn "$IAM_ROLE_ARN" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"s3://$BUCKET/jobs/jobv2.py\",
      \"sparkSubmitParameters\": \"$SPARK_PARAMS\"
    }
  }" \
  --configuration-overrides "{
    \"monitoringConfiguration\": {
      \"s3MonitoringConfiguration\": {
        \"logUri\": \"s3://$BUCKET/logs/\"
      }
    }
  }"
```

---

## Data Model

### DynamoDB Schema
**Table**: `dev-user-profiles`

```json
{
  "user_id": "user_001",           // HASH key
  "created_at": 1703851800000,     // RANGE key
  "email": "john@example.com",
  "status": "active",
  "properties": {                  // Nested JSON
    "profile": { "first_name": "John", "age": 32 },
    "subscription": { "plan": "premium", "price": 99.99 },
    "usage_stats": { "total_logins": 1234 }
  }
}
```

### S3 CDC Events
**Path**: `s3://bucket/user_profiles/year=2024/month=12/day=29/hour=15/events_*.json.gz`

```json
{
  "event_id": "abc123",
  "event_name": "INSERT",
  "new_image": { "user_id": "user_001", "email": "...", "properties": {...} },
  "processed_timestamp": "2024-12-29T10:30:00"
}
```

### Iceberg Table
**Table**: `s3tablesbucket.dynamodb.user_profiles`

```sql
CREATE TABLE user_profiles (
    user_id STRING,
    created_at BIGINT,
    email STRING,
    status STRING,              -- Partition key
    properties VARIANT,         -- Nested JSON as VARIANT
    event_timestamp TIMESTAMP
) PARTITIONED BY (status)
TBLPROPERTIES ('format-version' = '3');
```

**Query Example**:
```sql
SELECT 
    user_id,
    variant_get(properties, '$.profile.first_name', 'string') AS first_name,
    variant_get(properties, '$.subscription.plan', 'string') AS plan
FROM s3tablesbucket.dynamodb.user_profiles;
```

---

## Components

### Lambda CDC Processor (`handler.py`)
- **Trigger**: DynamoDB Streams (batch: 100 records)
- **Function**: Parses DynamoDB format → compresses to JSON.GZ
- **Output**: Time-partitioned files in S3

### Spark ETL Job (`ingest.py`)
1. **Create manifest** - Lists all `.json.gz` files
2. **Read & parse** - Loads compressed JSON from S3
3. **Transform** - Extracts `new_image`, converts `properties` to VARIANT
4. **Deduplicate** - Keeps latest record per `user_id + created_at`
5. **MERGE INTO** - Upserts into Iceberg table
6. **Archive** - Moves processed files to `archived/`

---

## Project Structure

```
.
├── serverless.yml       # Infrastructure (DynamoDB, Lambda, S3)
├── handler.py           # Lambda CDC processor
├── sample_data.py       # Test data generator
├── ingest.py            # Spark ETL job
├── submit_jobs.sh       # EMR job submission
└── README.md
```

---

## Cleanup

```bash
# Remove serverless stack
npx serverless remove --stage dev --region us-west-2

# Delete EMR application
aws emr-serverless delete-application --application-id $APPLICATION_ID

# Delete S3 Tables bucket
aws s3tables delete-table-bucket --name soumil-demo --region us-west-2
```
