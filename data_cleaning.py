import pandas as pd


class DataClean():
    
    def clean_user_data(self,de = pd.DataFrame()):
        df = de.extract_rds_table()
        df = df.dropna()
        return df 






