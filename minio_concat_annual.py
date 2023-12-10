import io
import pandas as pd
from minio import Minio

# Set your Minio server information
minio_url = "localhost:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
bucket_name = "parquets"
destination_bucket_name = "queries"

# Create Minio client
minio_client = Minio(minio_url, access_key, secret_key, secure=False)

# List all objects in the bucket
objects = minio_client.list_objects(bucket_name)

# Initialize an empty list to store DataFrames
dfs = []

# Loop through each Parquet file in the bucket
for obj in objects:
    # Check if the file name contains "2022" or "2023"
    if '2022' in obj.object_name or '2023' in obj.object_name:
        # Download the Parquet file
        file_data = minio_client.get_object(bucket_name, obj.object_name)
        parquet_data = io.BytesIO(file_data.read())

        # Read the Parquet file into a DataFrame
        df = pd.read_parquet(parquet_data)

        # Append the DataFrame to the list
        dfs.append(df)

# Combine all DataFrames into a single DataFrame
combined_df = pd.concat(dfs, ignore_index=True)

# Display the combined DataFrame
print(combined_df)

# Check if destination bucket exists, create it if not
if not minio_client.bucket_exists(destination_bucket_name):
    minio_client.make_bucket(destination_bucket_name)

# Upload query to bucket
try:
    # Convert the DataFrame to Parquet format
    parquet_data = io.BytesIO()
    combined_df.to_parquet(parquet_data, index=False)
    # Reset the buffer position to the beginning
    parquet_data.seek(0)
    # Upload the Parquet data to the destination bucket
    minio_client.put_object(destination_bucket_name, 'annual_data.parquet', parquet_data, parquet_data.getbuffer().nbytes)
    print("'Annual Data' parquet uploaded to 'queries' bucket successfully!")

except ReferenceError as err:
    print(f"Error uploading data: {err}")