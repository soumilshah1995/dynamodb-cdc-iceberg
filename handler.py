import json
import gzip
import boto3
import os
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any

s3_client = boto3.client('s3')

S3_BUCKET = os.environ['S3_BUCKET']
S3_PREFIX = os.environ.get('S3_PREFIX', 'user_profiles')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')


class DecimalEncoder(json.JSONEncoder):
    """Handle Decimal types from DynamoDB"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def parse_dynamodb_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse DynamoDB item to regular Python dict
    Handles nested structures in properties field
    """
    parsed = {}

    for key, value in item.items():
        if 'S' in value:
            parsed[key] = value['S']
        elif 'N' in value:
            parsed[key] = float(value['N'])
        elif 'BOOL' in value:
            parsed[key] = value['BOOL']
        elif 'NULL' in value:
            parsed[key] = None
        elif 'M' in value:
            # Nested map - recursively parse
            parsed[key] = parse_dynamodb_item(value['M'])
        elif 'L' in value:
            # List - parse each element
            parsed[key] = [parse_dynamodb_value(v) for v in value['L']]
        elif 'SS' in value:
            parsed[key] = value['SS']
        elif 'NS' in value:
            parsed[key] = [float(n) for n in value['NS']]

    return parsed


def parse_dynamodb_value(value: Dict[str, Any]) -> Any:
    """Parse a single DynamoDB attribute value"""
    if 'S' in value:
        return value['S']
    elif 'N' in value:
        return float(value['N'])
    elif 'BOOL' in value:
        return value['BOOL']
    elif 'NULL' in value:
        return None
    elif 'M' in value:
        return parse_dynamodb_item(value['M'])
    elif 'L' in value:
        return [parse_dynamodb_value(v) for v in value['L']]
    elif 'SS' in value:
        return value['SS']
    elif 'NS' in value:
        return [float(n) for n in value['NS']]
    return value


def process_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a single DynamoDB stream record
    Returns enriched event with CDC metadata
    """
    event_name = record['eventName']  # INSERT, MODIFY, REMOVE
    event_id = record['eventID']
    event_source_arn = record['eventSourceARN']

    # Extract table name from ARN
    table_name = event_source_arn.split('/')[-3]

    processed_event = {
        'event_id': event_id,
        'event_name': event_name,
        'event_source': 'dynamodb',
        'table_name': table_name,
        'environment': ENVIRONMENT,
        'processed_timestamp': datetime.utcnow().isoformat(),
        'approximate_creation_datetime': record.get('dynamodb', {}).get('ApproximateCreationDateTime')
    }

    dynamodb_data = record.get('dynamodb', {})

    # Parse new image (current state)
    if 'NewImage' in dynamodb_data:
        processed_event['new_image'] = parse_dynamodb_item(dynamodb_data['NewImage'])

    # Parse old image (previous state)
    if 'OldImage' in dynamodb_data:
        processed_event['old_image'] = parse_dynamodb_item(dynamodb_data['OldImage'])

    # Add keys for partitioning
    keys = dynamodb_data.get('Keys', {})
    if keys:
        processed_event['keys'] = parse_dynamodb_item(keys)

    return processed_event


def lambda_handler(event, context):
    """
    Lambda handler for DynamoDB CDC events
    Processes batch of records and writes to S3 as compressed JSON
    """
    try:
        records = event.get('Records', [])

        if not records:
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No records to process'})
            }

        # Process all records
        processed_events = []
        for record in records:
            try:
                processed_event = process_record(record)
                processed_events.append(processed_event)
            except Exception as e:
                print(f"Error processing record {record.get('eventID')}: {str(e)}")
                # Continue processing other records
                continue

        if not processed_events:
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No records successfully processed'})
            }

        # Generate S3 key with partitioning
        now = datetime.utcnow()
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')
        hour = now.strftime('%H')
        timestamp = now.strftime('%Y%m%d_%H%M%S_%f')

        # S3 key: user_profiles/year=2024/month=12/day=29/hour=15/events_20241229_153045_123456.json.gz
        s3_key = f"{S3_PREFIX}/year={year}/month={month}/day={day}/hour={hour}/events_{timestamp}.json.gz"

        # Convert to newline-delimited JSON
        json_lines = '\n'.join([
            json.dumps(event, cls=DecimalEncoder)
            for event in processed_events
        ])

        # Compress with gzip
        compressed_data = gzip.compress(json_lines.encode('utf-8'))

        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=compressed_data,
            ContentType='application/json',
            ContentEncoding='gzip',
            Metadata={
                'record_count': str(len(processed_events)),
                'environment': ENVIRONMENT,
                'processed_at': now.isoformat()
            }
        )

        print(f"Successfully processed {len(processed_events)} records")
        print(f"Written to s3://{S3_BUCKET}/{s3_key}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'records_processed': len(processed_events),
                's3_location': f"s3://{S3_BUCKET}/{s3_key}"
            })
        }

    except Exception as e:
        print(f"Error in lambda handler: {str(e)}")
        raise