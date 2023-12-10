import io
from minio import Minio
import pyarrow.parquet as pq

minio_client = Minio(endpoint='localhost:9000',
                    access_key='minioadmin',
                    secret_key='minioadmin',
                    secure=False)

bucket_name='parquets'
file_name='dataset.csv.parquet'
column= input('  Enter column to search : ')
value= input('  Enter value to search  : ')

response = minio_client.get_object(bucket_name, file_name)
parquet_file = pq.ParquetFile(io.BytesIO(response.read()))
df = parquet_file.read().to_pandas()
# Check if the specified column exists in the DataFrame
if column not in df.columns:
    print(f"The column '{column}' was not found in the DataFrame. Please check the column name and try again.")
else:
    result = df[df[column].str.contains(value, case=False, na=False)]
    if not result.empty:
        print(result)
    else:
        print("Result not found")