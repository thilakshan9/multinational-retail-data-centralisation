from data_extraction import DataExtractor
from database_utils import DatabaseConnector
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
        users['date_of_birth'] = pd.to_datetime(users['date_of_birth'], infer_datetime_format=True, errors='coerce')
        users = users.dropna(subset=['date_of_birth'])
        users['join_date'] = pd.to_datetime(users['join_date'], infer_datetime_format=True, errors='coerce')
        users = users.dropna(subset=['join_date'])
        users = users.reset_index(drop=True)
        # More than one person cannot have the same number - duplicates recognised via subset - kept the first instance and deleted others
        users = users.set_index('join_date')
        users = users.sort_index(ascending=True)
        print(f"Joined {users}")
        duplicated_phone = users.duplicated(subset=['phone_number'], keep = 'first')
        users = users[~duplicated_phone]
        users = users.reset_index()
        print(users)
        # Replacing GGB with GB - inferring
        users['country_code'] = users['country_code'].replace({'GGB':'GB'})
        users['address'] = users['address'].str.replace("\n"," ").str.replace("\\","").str.replace("/","")
        # Used the following to get the domain name extensions
        domain = users['email_address'].apply(lambda x: x.split('@')[1])
        domain = domain.apply(lambda x: x.split('.',1)[-1])
        # Emails must contain @ and domain name extension
        users = users[users['email_address'].str.contains('@')]
        users = users[users['email_address'].str.contains('.co.uk|.com|.de|.org|.info|.net|.biz')]
        # users['phone_raw'] = users['phone_number'].str.replace('\W', '', regex=True)
        # users = users[users['phone_raw'].str.isnumeric()]
        users = users.reset_index(drop=True)
        return users

data_2 = DatabaseConnector()
data_3 = DataExtractor()
users = data_3.read_rds_table(data_2, 'legacy_users')
data_4 = DataCleaning()
# data_4.clean_user_data(users)
users.to_string('users_data.txt')
users = data_4.clean_user_data(users)
users.to_string('cleaned_data.txt')
# print(f"Sum is:  {data_4.clean_user_data(users)}")

