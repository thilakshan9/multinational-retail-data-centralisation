from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd

class DataCleaning:
    def __init__(self):
        pass
    def clean_user_data(self, users):
        # Checking for any overall null values
        print(f"Total null values in the dataframe are : {users.isna().sum().sum()}")
        print(users.dtypes)
        users['country'] = users['country'].astype('category')
        users['country_code'] = users['country_code'].astype('category')
        print(users[users['date_of_birth'].str.match('[0-9]{4}-[0-9]{2}-[0-9]{2}')== False])
        print(users.loc[360,'date_of_birth'])
        print(users.head())
        users['date_of_birth'] = pd.to_datetime(users['date_of_birth'], infer_datetime_format=True, errors='coerce')
        users = users.dropna(subset=['date_of_birth'])
        users = users.reset_index(drop=True)
        users['join_date'] = pd.to_datetime(users['join_date'], infer_datetime_format=True, errors='coerce')
        users = users.dropna(subset=['join_date'])
        users = users.reset_index(drop=True)
        print(f"Duplicaed is {users.duplicated(subset=['phone_number'],keep=False).sum()}")
        print(users[users.duplicated(subset='phone_number',keep=False)])
        print(users[users['country_code']=='GGB'])
        users['country_code'] = users['country_code'].replace({'GGB':'GB'})
        print(users[users['country_code']=='GGB'])
        users['address'] = users['address'].str.replace("\n"," ").str.replace("\\","").str.replace("/","")
        print(users['country'].unique())
        # Checking the email address are all in the correct format
        domain = users['email_address'].apply(lambda x: x.split('@')[1])
        domain = domain.apply(lambda x: x.split('.',1)[-1])
        print(domain.unique())
        print(users[~users['email_address'].str.contains('@')].shape[0])
        users = users[users['email_address'].str.contains('@')]
        print(users[~users['email_address'].str.contains('.co.uk|.com|.de|.org|.info|.net|.biz')].shape[0])
        print(users[users['email_address'].str.contains('.co.uk|.com|.de|.org|.info|.net|.biz')])
        print(f"Address is {users['address']}")
        address_check = users['address'].apply(lambda x: x.split('\n'))
        print(address_check)
        users['phone_raw'] = users['phone_number'].str.replace('\W', '', regex=True)
        users = users[users['phone_raw'].str.isnumeric()]
        users = users.reset_index(drop=True)
        print(users)
        # print(users.loc[360, 'date_of_birth'])
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

