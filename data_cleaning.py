import pandas as pd


class DataClean():
    
    def clean_user_data(self,de = pd.DataFrame()):
        df = de.extract_rds_table()
        df = df.dropna()
        return df 

    def clean_card_data(self, de = pd.DataFrame()):
        df = df.dropna()
        return df    

    def clean_store_data(self, df = pd.DataFrame()):
        df = df.dropna() 
        return df   






