import pandas as pd 
from database_utils import DatabaseConnector
import sqlalchemy
import psycopg2
import yaml
import tabula

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
   dfs = tabula.read_pdf(link, stream=True)
   return dfs 







if __name__ == "__main__":
    dum = DataExtractor()
    PDF_PATH = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    cred_tb = dum.retrieve_pdf_data('card_details.pdf')
    type(cred_tb)


    
