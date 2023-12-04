from minio import Minio
from minio.error import ResponseError

def upload_to_minio(file_path, object_name, bucket_name, endpoint, access_key, secret_key):
    try:
        # Initialize the Minio client
        minio_client = Minio(endpoint,
                             access_key=access_key,
                             secret_key=secret_key,
                             secure=False)  # Change to True if using HTTPS

        # Check if the bucket exists, create it if not
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        # Upload the file
        minio_client.fput_object(bucket_name, object_name, file_path)

        print(f"File '{object_name}' uploaded successfully to bucket '{bucket_name}' on Minio server.")

    except ResponseError as e:
        print(f"Error: {e}")

# Set your Minio server details
minio_endpoint = "http://localhost:9000"  # Update with your Minio server endpoint
minio_access_key = "your_access_key"
minio_secret_key = "your_secret_key"
minio_bucket_name = "your_bucket_name"

# File to upload
file_path_to_upload = "/path/to/your/file.txt"
object_name_in_minio = "uploaded_file.txt"

# Call the function
upload_to_minio(file_path_to_upload, object_name_in_minio, minio_bucket_name, minio_endpoint, minio_access_key, minio_secret_key)