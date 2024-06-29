import requests
import json
import boto3
import os

RAWG_API_KEY = "c65e236ae356431e8eabf1d6a3fc2136"
BUCKET_NAME = 'rawg-data-analytics'
FILE_NAME = 'games_2023.json'
REGION = 'ap-south-1'  # Change to your preferred region


# Fetch data from RAWG API
def fetch_games_from_rawg():
    base_url = 'https://api.rawg.io/api/games'
    start_date = '2023-01-01'
    end_date = '2023-12-31'
    page = 1
    page_size = 40  # Maximum allowed page size
    all_games = []

    while True:
        url = f'{base_url}?dates={start_date},{end_date}&key={RAWG_API_KEY}&page={page}&page_size={page_size}'
        response = requests.get(url)
        data = response.json()

        if 'results' in data and data['results']:
            all_games.extend(data['results'])
            print(f'Fetched page {page}')
        else:
            break

        page += 1

    return all_games


# Upload data to S3
def upload_to_s3(data, bucket_name, file_name, region):
    s3 = boto3.client('s3', region_name=region)
    json_data = json.dumps(data)

    s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_data)
    print(f'Uploaded data to S3 bucket {bucket_name} as {file_name}')


# Main function
if __name__ == '__main__':
    games_data = fetch_games_from_rawg()
    print(f'Total games fetched: {len(games_data)}')

    upload_to_s3(games_data, BUCKET_NAME, FILE_NAME, REGION)

