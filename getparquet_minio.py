import boto3
import io
import pandas as pd
import os

# Set your Minio credentials and endpoint
minio_credentials = {
    'region': 'us-east-1',
    'endpoint': 'http://10.183.16.169:9000',
    'use_ssl': False,
    'url_style': 'path',
    'access_key': 'readonly',
    'secret_key': 'readonly'
}

# Create an S3 client
s3 = boto3.client(
    's3',
    region_name=minio_credentials['region'],
    endpoint_url=minio_credentials['endpoint'],
    aws_access_key_id=minio_credentials['access_key'],
    aws_secret_access_key=minio_credentials['secret_key'],
    use_ssl=minio_credentials['use_ssl'],
    config=boto3.session.Config(signature_version='s3v4')
)

# Set the directory to save files
save_directory = './data'

# Create the directory if it doesn't exist
if not os.path.exists(save_directory):
    os.makedirs(save_directory)

# List all buckets
response = s3.list_buckets()

# Print each bucket name
print("list of buckets")
for bucket in response['Buckets']:
    print(bucket['Name'])
    objects = s3.list_objects(Bucket=bucket['Name'])
    
    for obj in objects['Contents']:
        print('Downloading ' + obj['Key'])
        
        if os.path.isfile(os.path.join(save_directory, obj['Key'].split('/')[-1] + '.parquet')):
            print(obj['Key'] + ' already exists, skipping...')
            continue

        data = io.BytesIO()
        s3.download_fileobj(bucket['Name'], obj['Key'], data)

        # Save CSV file in the ./data directory
        with open(os.path.join(save_directory, obj['Key'].split('/')[-1]), 'wb') as csv_file:
            csv_file.write(data.getvalue())

        print(obj['Key'] + ' downloaded and saved as CSV in the ./data directory')
        
        # Change the data back to String IO for further processing if needed
        data = io.StringIO(data.getvalue().decode('utf-8'))

        print(obj['Key'] + ' downloaded, now turning it into a parquet file')

        df = pd.read_csv(data)
        df.to_parquet(os.path.join(save_directory, obj['Key'].split('/')[-1] + '.parquet'), index=False)