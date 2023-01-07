import pandas as pd 
from database_utils import DatabaseConnector
import sqlalchemy
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
    local_db = data['LOCAL_DB']
    new_eng = sqlalchemy.create_engine(local_db)
    new_eng.connect()
    
    





if __name__ == "__main__":
    dum = DataExtractor()
    df = dum.extract_rds_table()
    print(df.head(10))


    
