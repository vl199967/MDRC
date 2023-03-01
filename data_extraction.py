import pandas as pd 
from database_utils import DatabaseConnector
import sqlalchemy
import boto3  
import psycopg2
import yaml
import tabula 
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from data_cleaning import DataClean
import re 
import string as st
import numpy as np

class DataExtractor():
   
 def extract_rds_table(self,dbcon = DatabaseConnector()):
    eng = dbcon.init_db_engine()
    user_tb = dbcon.list_db_tables()
    users = pd.read_sql_table(user_tb[1],eng)
    return users

 def upload_to_db(self,df,tb_name): ##task 3 step 7&8 to be completed 
    with open('db_creds.yaml') as f:
         data = yaml.safe_load(f)
    engine = sqlalchemy.create_engine(f"{'postgresql'}://{data['LOCAL_USER']}:{data['LOCAL_PASS']}@{data['LOCAL_HOST']}:{data['LOCAL_PORT']}/{data['LOCAL_DB']}")
    df.to_sql(tb_name,engine,if_exists='replace')
    
 def retrieve_pdf_data(self,link):
   dfs = tabula.read_pdf(link, pages='all', stream=True)
   return pd.DataFrame(dfs) 

 def list_number_of_stores(self):
   with open('db_creds.yaml') as f:
         data = yaml.safe_load(f)
   header ={"x-api-key": data['X_API_KEY']}      
   res = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',headers = header)
   res = res.json()
   return res['number_stores']
 
 def retrieve_stores_data(self):
   df = []
   number_stores = self.list_number_of_stores()
   with open('db_creds.yaml') as f:
         data = yaml.safe_load(f)
   header ={"x-api-key": data['X_API_KEY']}
   for i in range(number_stores):
    res = requests.get(' https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'.format(store_number = i),headers = header)
    print("currently working on the {i}th item".format(i=i))
    df.append(res.json()) 
   return pd.DataFrame(df)      

 def extract_from_s3(self):
   s3_client = boto3.client('s3')
   df = s3_client.download_file('data-handling-public','products.csv','products.csv')
   return df


if __name__ == "__main__":
    with open('db_creds.yaml') as f:
      data = yaml.safe_load(f)
    cleaner = DataClean()
    dum = DataExtractor()
    header ={"x-api-key": data['X_API_KEY']}  

    '''
    df = pd.read_csv('processed_users (1).csv')
    df = df.dropna(axis=0,how='any')
    regex = re.compile('^[A-Z0-9]{10}$')
    rm = df[~df['first_name'].str.contains(regex)]
    dum.upload_to_db(rm,'dim_users')
    '''
    '''
    df = pd.read_csv('dim_store_details.csv')
    df = df.drop(labels=[63,172,231,333,381,414,447],axis=0)
    dum.upload_to_db(df,'dim_store_details')
    '''

    df = pd.read_csv('products.csv')
    df = df.dropna(axis=0,how='any')
    df = df.drop(labels=[751,1133,1400],axis=0)
    df['product_price'] = df['product_price'].str.lstrip('£')

    

    
    wgt = [x for x in df['weight']]
    processed = [float(str(x).rstrip('kg')) if re.search('kg$',str(x)) 
                                            else float(str(x).partition('ml')[0])/1000
                                            if re.search('ml',str(x))
                                            else float(str(x).partition('x')[0])*float(str(x).partition('x')[2].rstrip('g'))/1000
                                            if re.search('x',str(x))
                                            else float(str(x).rstrip('oz')) * 0.0283
                                            if re.search('oz',str(x))
                                            else float(str(x).rstrip('g .'))/1000 
                                            for x in wgt ]    

    df['weight_class'] = [ 'Light' if x <= 2
                                   else 'Mid_Sized' if x >2 and x<= 40 
                                   else 'Heavy' if x>40 and x<=140
                                   else 'Truck_Required'
                            for x in processed]
    df['weight'] = processed 
    dum.upload_to_db(df,'dim_products')                     
                                                       




  






    
  
   
    

    
