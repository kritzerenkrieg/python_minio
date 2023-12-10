import os
from minio import Minio

# Specify bucket and downloaded file path
bucket_name = "parquets"
folder_path = "./downloaded"

# Set your Minio server  credentials
minio_client = Minio(endpoint='localhost:9000',
                    access_key='minioadmin',
                    secret_key='minioadmin',
                    secure=False)

if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

# Loop check for all downloaded files
for filename in os.listdir(folder_path):
    if filename.endswith(".parquet"):
        file_path = os.path.join(folder_path, filename)
        object_name = filename
        minio_client.fput_object(bucket_name, object_name, file_path)
        print(f"File '{object_name}' uploaded sucessfully to bucket '{bucket_name}' on local Minio server.")