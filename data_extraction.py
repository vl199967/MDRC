import pandas as pd 
from database_utils import DatabaseConnector
import sqlalchemy
import psycopg2
import yaml
import tabula
import requests

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
   number_stores = self.list_number_of_stores()
   with open('db_creds.yaml') as f:
         data = yaml.safe_load(f)
   header ={"x-api-key": data['X_API_KEY']}
   res = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'.format(store_number = number_stores),headers = header)
   return res.json()       
   







if __name__ == "__main__":
    dum = DataExtractor()
    '''
    PDF_PATH = 'card_details.pdf'
    cred_tb = dum.retrieve_pdf_data(PDF_PATH)
    dum.upload_to_db(cred_tb,'dim_card_details')
    '''
    print(dum.retrieve_stores_data())

    
