# Import necessary libraries
import numpy as np
import pandas as pd
from minio import Minio
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from yellowbrick.cluster import KElbowVisualizer
import os
import io

# Set LOKY_MAX_CPU_COUNT to the number of cores you want to use
os.environ["LOKY_MAX_CPU_COUNT"] = "4"  # Adjust the number as needed

# Function to load data from Minio
def load_data_from_minio(bucket_name, file_name):
    minio_client = Minio("localhost:9000",
                         access_key="minioadmin",
                         secret_key="minioadmin",
                         secure=False)  # Change to True if using HTTPS

    # Get the Parquet file as an object
    obj = minio_client.get_object(bucket_name, file_name)

    # Read the Parquet file content
    parquet_content = obj.read()

    # Assuming 'parquet_content' contains the raw Parquet data as bytes
    # Read Parquet data using BytesIO
    parquet_stream = io.BytesIO(parquet_content)

    # Read the Parquet data using pandas
    df = pd.read_parquet(parquet_stream)

    return df

# Function for efficient clustering
def efficient_clustering(df, features, num_clusters_range=(2, 10)):
    # Select features for clustering
    X = df[features]

    # Standardize the features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Use the Elbow method to find optimal number of clusters
    model = KMeans()
    visualizer = KElbowVisualizer(model, k=num_clusters_range)
    visualizer.fit(X_scaled)
    optimal_clusters = visualizer.elbow_value_

    # Perform KMeans clustering with optimal number of clusters
    kmeans = KMeans(n_clusters=optimal_clusters, random_state=42)
    labels = kmeans.fit_predict(X_scaled)

    # Add cluster labels to the DataFrame
    df['Cluster_Labels'] = labels

    return df, optimal_clusters

# Function to visualize clustering results
def visualize_clusters(df, x_feature, y_feature, hue_feature='Cluster_Labels'):
    plt.figure(figsize=(12, 8))
    sns.scatterplot(data=df, x=x_feature, y=y_feature, hue=df[hue_feature],
                    palette=sns.color_palette('hls', df[hue_feature].nunique()))
    plt.title(f'Clustering Results - {x_feature} vs {y_feature}')
    plt.show()

# Example usage
if __name__ == "__main__":
    # Specify the bucket and file information
    bucket_name = "parquets"
    file_name = "heart_attack_prediction_dataset.csv.parquet"

    # Load data from Minio
    data_df = load_data_from_minio(bucket_name, file_name)

    # Features for clustering
    clustering_features = ['Age', 'Cholesterol', 'Heart Rate', 'Diabetes', 'Smoking', 'BMI', 'Alcohol Consumption', 'Stress Level']

    # Perform efficient clustering
    clustered_df, optimal_clusters = efficient_clustering(data_df, features=clustering_features)

    # Visualize clustering results
    visualize_clusters(clustered_df, x_feature='Cholesterol', y_feature='Age')
