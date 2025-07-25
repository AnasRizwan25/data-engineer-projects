import boto3
from dotenv import load_dotenv
import os

load_dotenv()

s3 = boto3.client('s3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-1"
)

bucket = "bucket-name"  
key = "stock_data.csv"
file_path = "data/stock_data.csv"

s3.upload_file(file_path, bucket, key)
print("Uploaded to S3 ✅")
