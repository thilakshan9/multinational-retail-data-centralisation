import pandas as pd
class DataCleaning:
    def __init__(self):
        pass
    def clean_user_data(self, users):
        # Checking for any overall null values - none present
        print(f"Total null values in the dataframe are : {users.isna().sum().sum()}")
        # Printing the data types to see if they are of the right type
        print(users.dtypes)
        users['date_of_birth'].to_string('dob_frame.txt')
        users['country'] = users['country'].astype('category')
        users['country_code'] = users['country_code'].astype('category')
        date_pattern = r'\b(\d{4}[-/]\d{2}[-/]\d{2}|\d{4} (?:January|February|March|April|May|June|July|August|September|October|November|December) \d{2}|\d{1,2} (?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2})\b'
        print(users['date_of_birth'].loc[1023])
        print(users['date_of_birth'].loc[867])
        users['date_of_birth'] = users['date_of_birth'].str.extract(date_pattern, expand=False)
        users = users[users['date_of_birth'].notnull()]
        # users= users[~users['date_of_birth'].isna()]
        print("ISNA" + users['date_of_birth'][users['date_of_birth'].isna()])
        users['date_of_birth'] = pd.to_datetime(users['date_of_birth'], format="%Y-%m-%d", errors='coerce')
        users['join_date'] = users['join_date'].str.extract(date_pattern, expand=False)
        users = users[users['join_date'].notnull()]
        users = users.dropna(subset='join_date')
        users['join_date'] = pd.to_datetime(users['join_date'], format="%Y-%m-%d", errors='coerce')
        users = users.reset_index(drop=True)
        # Replacing GGB with GB - inferring
        users['country_code'] = users['country_code'].replace({'GGB':'GB'})
        users['address'] = users['address'].str.replace("\n"," ").str.replace("\\","").str.replace("/","")
        # Used the following to get the domain name extensions
        domain = users['email_address'].apply(lambda x: x.split('@')[1])
        domain = domain.apply(lambda x: x.split('.',1)[-1])
        # Emails must contain @ and domain name extension
        users = users[users['email_address'].str.contains('@')]
        users = users[users['email_address'].str.contains('.co.uk|.com|.de|.org|.info|.net|.biz')]
        users = users.reset_index(drop=True)
        users.to_string('cleaned_users.txt')
        return users
    def clean_card_data(self, df):
        print(f"Total null values in the dataframe are : {df.isna().sum().sum()}")
        print(df['card_provider'].unique())
        df['card_provider'] = df['card_provider'].astype('category')
        df['card_number'] =  df['card_number'].astype(int)
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df.to_string('clean_card.txt')
        return df
    def called_clean_store_date(self,df):
        print(df['continent'].unique())
        df['continent'] = df['continent'].replace({'eeAmerica':'America', 'eeEurope':'Europe'})
        print(df['store_type'].unique())


   
 