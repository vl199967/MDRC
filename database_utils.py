import yaml
import sqlalchemy
from sqlalchemy import inspect
import psycopg2


class DatabaseConnector():
    

    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            data = yaml.safe_load(f)
        return data    

    def init_db_engine(self):
        creds = self.read_db_creds()
        engine = sqlalchemy.create_engine(f"{'postgresql'}+{creds['DBAPI']}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine 


    def list_db_tables(self):
        eng = self.init_db_engine()
        eng.connect()
        inspector = inspect(eng)
        return inspector.get_table_names()     
      

if __name__ == "__main__":
    dummy = DatabaseConnector()  
    dummy.read_db_creds()
    dummy.init_db_engine()
    bruh = dummy.list_db_tables()
    print(bruh)

