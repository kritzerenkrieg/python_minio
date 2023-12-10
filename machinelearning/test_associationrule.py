import io
import os
from minio import Minio
import pandas as pd 
from mlxtend.frequent_patterns import apriori, association_rules 
# Initialize the Minio client

minio_client = Minio(   endpoint="localhost:9000",
                        access_key="minioadmin",
                        secret_key="minioadmin",
                        secure=False)  # Change to True if using HTTPS

# Specify the bucket and file information
bucket_name = "parquets"
file_name = "Online_Retail.csv.parquet"

# Get the Parquet file as an object
obj = minio_client.get_object(bucket_name, file_name)
    
# Read the Parquet file content
parquet_content = obj.read()

# Assuming 'parquet_content' contains the raw Parquet data as bytes
# Read Parquet data using BytesIO
parquet_stream = io.BytesIO(parquet_content)

# Read the Parquet data using pandas
df = pd.read_parquet(parquet_stream)

# Cleaning the Data
# Stripping extra spaces in the description 
df['Description'] = df['Description'].str.strip() 
  
# Dropping the rows without any invoice number 
df.dropna(axis = 0, subset =['InvoiceNo'], inplace = True) 
df['InvoiceNo'] = df['InvoiceNo'].astype('str') 
  
# Dropping all transactions which were done on credit 
df = df[~df['InvoiceNo'].str.contains('C')] 

# Splitting the data according to the region of transaction
# Transactions done in France 
basket_France = (df[df['Country'] =="France"] 
          .groupby(['InvoiceNo', 'Description'])['Quantity'] 
          .sum().unstack().reset_index().fillna(0) 
          .set_index('InvoiceNo')) 
  
# Transactions done in the United Kingdom 
basket_UK = (df[df['Country'] =="United Kingdom"] 
          .groupby(['InvoiceNo', 'Description'])['Quantity'] 
          .sum().unstack().reset_index().fillna(0) 
          .set_index('InvoiceNo')) 
  
# Transactions done in Portugal 
basket_Por = (df[df['Country'] =="Portugal"] 
          .groupby(['InvoiceNo', 'Description'])['Quantity'] 
          .sum().unstack().reset_index().fillna(0) 
          .set_index('InvoiceNo')) 
  
# Transactions done in Sweden     
basket_Sweden = (df[df['Country'] =="Sweden"] 
          .groupby(['InvoiceNo', 'Description'])['Quantity'] 
          .sum().unstack().reset_index().fillna(0) 
          .set_index('InvoiceNo')) 

# Hot encoding the Data

# Defining the hot encoding function to make the data suitable  
# for the concerned libraries 
def hot_encode(x): 
    if(x<= 0): 
        return 0
    if(x>= 1): 
        return 1
  
# Encoding the datasets 
basket_encoded = basket_France.map(hot_encode) 
basket_France = basket_encoded 
  
basket_encoded = basket_UK.map(hot_encode) 
basket_UK = basket_encoded 
  
basket_encoded = basket_Por.map(hot_encode) 
basket_Por = basket_encoded 
  
basket_encoded = basket_Sweden.map(hot_encode) 
basket_Sweden = basket_encoded

# Building the models and analyzing the results
# Building the model 
frq_items = apriori(basket_France, min_support = 0.05, use_colnames = True) 
  
# Collecting the inferred rules in a dataframe 
rules = association_rules(frq_items, metric ="lift", min_threshold = 1) 
rules = rules.sort_values(['confidence', 'lift'], ascending =[False, False]) 
print(rules)

# Specifying the directory of result
results_directory = './test_results'

# Creating the directory if it doesn't exist
if not os.path.exists(results_directory):
    os.makedirs(results_directory)
    print(f"Directory '{results_directory}' created!")

# Write result to specified directory
rules.to_csv('./test_results/association_rules.csv', index=False)
print("saved result to ./test_results/association_rules.csv")