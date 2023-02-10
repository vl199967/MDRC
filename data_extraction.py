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
    dum.extract_from_s3()
    df = pd.read_csv('products.csv',index_col=0,header=0)
    dum.upload_to_db(df,'dim_products')

  
   
    

    
