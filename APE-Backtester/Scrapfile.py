from datetime import datetime, timedelta
from io import StringIO
import os
import boto3
import pandas as pd
import requests
import json
import ast
import holidays


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def s3_data():
    #Pulls training set data from s3
    s3 = boto3.resource('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    my_bucket = s3.Bucket('icarus-research-data/training_datasets/expanded_1d_datasets/2023/04')
    for file in my_bucket.objects.all():
        print(file.key)
    
    return
    # bucket_name = 'icarus-research-data'
    # object_key = 'training_datasets/expanded_1d_datasets/2023/04/17.csv'
    # obj = s3.get_object(Bucket = bucket_name, Key = object_key)
    # rawdata = obj['Body'].read().decode('utf-8')
    # df = pd.read_csv(StringIO(rawdata))
    # df.dropna(inplace = True)
    # df.reset_index(inplace= True, drop = True)
    # df['contracts'] = df['contracts'].apply(lambda x: ast.literal_eval(x))
    # df['contracts_available'] = df['contracts'].apply(lambda x: len(x)>=12)
    # return df

# s3_data()
print(holidays)