from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning:
    def __init__(self):
        pass
    def clean_user_data(self, users):
        return users.isna().sum().sum()

data_2 = DatabaseConnector()
data_3 = DataExtractor()
users = data_3.read_rds_table(data_2, 'legacy_users')
data_4 = DataCleaning()
print(f"Sum is:  {data_4.clean_user_data(users)}")

