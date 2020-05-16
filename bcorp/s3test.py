import boto3
import pandas as pd
from smart_open import open
s3 = boto3.resource('s3')
bucket_name = 'scrapemay16'
object_key = "scraped.csv"
path = f"s3://{bucket_name}/{object_key}"

df = pd.read_csv(open(path))
print(df.head())