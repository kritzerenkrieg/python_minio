import os
from minio import Minio
from minio.error import S3Error

def upload_parquet_files_to_minio(folder_path, bucket_name, endpoint, access_key, secret_key):
    try:
        # Initialize the Minio client
        minio_client = Minio(endpoint,
                             access_key=access_key,
                             secret_key=secret_key,
                             secure=False)  # Change to True if using HTTPS

        # Check if the bucket exists, create it if not
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        # Loop through files in the specified directory
        for filename in os.listdir(folder_path):
            if filename.endswith(".parquet"):
                file_path = os.path.join(folder_path, filename)
                object_name = filename

                # Upload the parquet file
                minio_client.fput_object(bucket_name, object_name, file_path)

                print(f"File '{object_name}' uploaded successfully to bucket '{bucket_name}' on Minio server.")

    except ReferenceError as e:
        print(f"Error: {e}" +"Too bad!")

# Set your Minio server details
minio_endpoint = "localhost:9000"
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
minio_bucket_name = "parquets"

# Directory containing parquet files
parquet_folder_path = "./data"

# Call the function
upload_parquet_files_to_minio(parquet_folder_path, minio_bucket_name, minio_endpoint, minio_access_key, minio_secret_key)