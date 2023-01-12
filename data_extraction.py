import pandas as pd 
from database_utils import DatabaseConnector
import sqlalchemy
import psycopg2
import yaml

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
    
    





if __name__ == "__main__":
    dum = DataExtractor()
    df = dum.extract_rds_table()
    print(df.head(10))
    dum.upload_to_db(df,'dim_users')


    
