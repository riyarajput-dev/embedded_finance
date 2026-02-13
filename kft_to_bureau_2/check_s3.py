import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    region_name=os.getenv("AWS_REGION")
)

bucket = "bureau-data-exchange"
prefixes = ["kft-to-bureau/equifax/logs/server_log.log", "kft-to-bureau/equifax/logs/server_log.txt"]

for key in prefixes:
    print(f"Checking {key}...")
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        print(f"Found {key}. Last 500 chars:")
        print(content[-500:])
    except Exception as e:
        print(f"Could not read {key}: {e}")
