import boto3
import json
import numpy as np
import pandas as pd
import os

# static variables

BUCKET_NAME = 'rawg-data-analytics'
RAW_DATA = 'games_2024.json'
CLEANED_DATA = 'games_2024_cleaned'
REGION = 'ap-south-1'

s3 = boto3.client('s3', region_name='ap-south-1')


def list_s3_buckets():
    # Initialize a session using Amazon S3
    s3_temp = boto3.client('s3')

    # Call S3 to list current buckets
    response = s3_temp.list_buckets()

    # Get a list of all bucket names from the response
    buckets = [bucket['Name'] for bucket in response['Buckets']]

    # Print out the bucket list
    print("Bucket List: %s" % buckets)


# function to load raw data from s3
def load_raw_data(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    raw_dataa = response['Body'].read().decode('utf-8')
    return json.loads(raw_dataa)


raw_data = load_raw_data(BUCKET_NAME, RAW_DATA)
print("Raw data is loaded successfully")


# Convert raw JSON data to pandas DataFrame
df = pd.json_normalize(raw_data)

print(df.head())
# Define target directory and file path
target_directory = './games_data'
file_name = 'data.csv'
file_path = os.path.join(target_directory, file_name)

# Ensure the directory exists
os.makedirs(target_directory, exist_ok=True)

# Save DataFrame to CSV in the target directory
df.to_csv(file_path, index=False)

print(f"DataFrame saved to {file_path}")

