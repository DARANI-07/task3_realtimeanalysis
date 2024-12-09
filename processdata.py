import json
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = "realtime-data-bucket"  
    transformed_records = []
    
    for record in event['Records']:
        payload = json.loads(record['kinesis']['data'])
        
        payload['status'] = "ok" if payload['temperature'] < 25 else "alert"
        
        transformed_records.append(payload)
    
    file_name = f"transformed_data_{int(time.time())}.json"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(transformed_records)
    )
    
    print(f"Saved transformed data to {bucket_name}/{file_name}")
    return {'statusCode': 200}
