from database_utils import DatabaseConnector
import pandas as pd
class DataExtractor:
    def __init__(self):
        pass
    def read_rds_table(self, connect, table_name):
            engine = connect.init_db_engine()
            users = pd.read_sql_query(f'''SELECT * FROM {table_name}''', engine).set_index('index')
            return users
    
data_1 = DataExtractor()
data_2 = DatabaseConnector()
print(data_1.read_rds_table(data_2, 'legacy_users'))