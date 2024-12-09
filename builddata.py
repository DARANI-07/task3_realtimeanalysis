import boto3
import json
import random
import time

kinesis_client = boto3.client('kinesis', region_name='us-east-1')

def generate_sensor_data():
    """Simulate sensor data."""
    return {
        "sensor_id": random.randint(1, 100),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(30.0, 50.0), 2),
        "timestamp": time.time()
    }

def main():
    stream_name = "RealTimeStream"  
    while True:
        data = generate_sensor_data()
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey="partition-1"
        )
        print(f"Sent: {data}")
        time.sleep(1)

if __name__ == "__main__":
    main()
