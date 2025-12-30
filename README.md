# dynamodb-cdc-iceberg
Repository: dynamodb-cdc-iceberg
This project contains code to capture DynamoDB Change Data Capture (CDC) events and write them to Iceberg tables, with EMR/Lambda orchestration.
Contents
- handler.py - Lambda entrypoint
- serverless.yml - Serverless Framework configuration
- spark_jobs/ - Spark job code
Usage
1. Configure AWS credentials and Serverless/SSH access for GitHub pushes.
2. Deploy with Serverless: `npx serverless deploy --stage dev` or `serverless deploy --stage dev` (Serverless v3 required).
License: MIT
