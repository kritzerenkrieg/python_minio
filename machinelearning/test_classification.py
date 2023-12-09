import os
import pandas as pd
import pyarrow.parquet as pq
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import boto3
from botocore.exceptions import NoCredentialsError
from minio import Minio
import matplotlib.pyplot as plt
import seaborn as sns

# Set your MinIO server credentials
minio_endpoint = "http://localhost:9000"
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
minio_bucket_name = "parquets"
minio_file_name = "heart_attack_prediction_dataset.csv.parquet"

# Download the Parquet file from MinIO
def download_file_from_minio():
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
        )

        with open(minio_file_name, "wb") as f:
            s3.download_fileobj(minio_bucket_name, minio_file_name, f)

    except NoCredentialsError:
        print("Credentials not available")

# Download the file
download_file_from_minio()

# Read the Parquet file into a Pandas DataFrame
table = pq.read_table(minio_file_name)
df = table.to_pandas()
df

# Data preprocessing
# Split 'Blood Pressure' into two separate columns
df[['Systolic Pressure', 'Diastolic Pressure']] = df['Blood Pressure'].str.split('/', expand=True)

# Convert to numeric
df['Systolic Pressure'] = pd.to_numeric(df['Systolic Pressure'], errors='coerce')
df['Diastolic Pressure'] = pd.to_numeric(df['Diastolic Pressure'], errors='coerce')

# Adjust the following based on your dataset columns
features = df[['Age', 'Cholesterol', 'Systolic Pressure', 'Diastolic Pressure', 'BMI', 'Physical Activity Days Per Week']]
target = df['Heart Attack Risk']

# Split the dataset
X_train, X_test, y_train, y_test = train_test_split(features, target, test_size=0.2, random_state=42)

# Normalize the data
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# Build and train the model 
model = GaussianNB()
model.fit(X_train, y_train)

y_pred = model.predict(X_test)

print("Accuracy:", accuracy_score(y_test, y_pred))
print("\nConfusion Matrix:\n", confusion_matrix(y_test, y_pred))
print("\nClassification Report:\n", classification_report(y_test, y_pred))

# Confusion Matrix
cm = confusion_matrix(y_test, y_pred)

# Plot Confusion Matrix and other results
plt.figure(figsize=(12, 8))

# Plot Confusion Matrix
plt.subplot(2, 2, 2)
sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", cbar=False)
plt.title("Confusion Matrix")
plt.xlabel("Predicted")
plt.ylabel("Actual")

# Plot Classification Report
plt.subplot(2, 2, 1)
plt.text(0, 0.5, classification_report(y_test, y_pred, zero_division=1), {'fontsize': 10}, fontfamily='monospace')
plt.axis('off')
plt.title('Classification Report')

# Specifying the directory of result
results_directory = './test_results'

# Save the plot as an image
results_image_path = os.path.join(results_directory, 'results_plot.png')
plt.tight_layout()  # Adjust layout to prevent overlap
plt.savefig(results_image_path)

# Display the plot (optional)
plt.show()

print(f"Results saved as an image: {results_image_path}")

# Creating the directory if it doesn't exist
if not os.path.exists(results_directory):
    os.makedirs(results_directory)
    print(f"Directory '{results_directory}' created!")