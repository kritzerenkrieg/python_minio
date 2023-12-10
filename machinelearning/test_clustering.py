from minio import Minio
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
import io
import os
import warnings

warnings.filterwarnings('ignore')

# Initialize the Minio client
minio_client = Minio("localhost:9000",
                     access_key="minioadmin",
                     secret_key="minioadmin",
                     secure=False)  # Change to True if using HTTPS

# Specify the bucket and file information
bucket_name = "parquets"
file_name = "heart_attack_prediction_dataset.csv.parquet"

# Get the Parquet file as an object and read it
obj = minio_client.get_object(bucket_name, file_name)
parquet_content = obj.read()

# Assuming 'parquet_content' contains the raw Parquet data as bytes
# Read Parquet data using BytesIO
parquet_stream = io.BytesIO(parquet_content)

# Read the Parquet data using pandas
df2 = pd.read_parquet(parquet_stream)

# Now you can work with the DataFrame 'df' containing your Parquet data
X2 = df2[['Age','Cholesterol','Heart Rate', 'Diabetes', 'Smoking', 'BMI', 'Alcohol Consumption', 'Stress Level']]
kmeans = KMeans(random_state=2)
y_kmeans = kmeans.fit_predict(X2)

# Make a cluster of two
km3 = KMeans(n_clusters=2).fit(X2)
X2['Labels'] = km3.labels_

# Specifying the directory of result
results_directory = './test_results'

# Creating the directory if it doesn't exist
if not os.path.exists(results_directory):
    os.makedirs(results_directory)
    print(f"Directory '{results_directory}' created!")

# Plot the clustered data
plt.figure(figsize=(12, 8))
sns.scatterplot(data=X2, x='Age', y='Cholesterol', hue=X2['Labels'], palette={0: 'blue', 1: 'red'})
plt.title('KMeans with 2 Clusters')
plt.savefig('./test_results/kmeans_clusters.png')  # Save the plot
plt.show()

# Print a message
print("Results exported successfully to the ./test_results directory.")
