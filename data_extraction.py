import pandas as pd 
from database_utils import DatabaseConnector


class DataExtractor():
   

   def extract_rds_table(self,dbcon = DatabaseConnector()):
    eng = dbcon.init_db_engine()
    user_tb = dbcon.list_db_tables()

    users = pd.read_sql_table(user_tb[1],eng)
    return users



if __name__ == "__main__":
    dum = DataExtractor()
    df = dum.extract_rds_table()
    print(df.head(10))


    
