
import pandas as pd

class DataCleaning:
    def __init__(self):
        pass
    def clean_user_data(self, users):
        # Checking for any overall null values - none present
        print(f"Total null values in the dataframe are : {users.isna().sum().sum()}")
        # Printing the data types to see if they are of the right type
        print(users.dtypes)
        users['country'] = users['country'].astype('category')
        users['country_code'] = users['country_code'].astype('category')
        users['date_of_birth'] = pd.to_datetime(users['date_of_birth'], errors='coerce')
        users = users.dropna(subset=['date_of_birth'])
        users['join_date'] = pd.to_datetime(users['join_date'], errors='coerce')
        users = users.dropna(subset=['join_date'])
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
        return users
   
    

