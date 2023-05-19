import yaml
from sqlalchemy import create_engine, inspect
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

class DatabaseConnector:
    def __init__(self):
        pass
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as f:
            creds_dict = yaml.safe_load(f)
        return creds_dict
    def init_db_engine(self):
        creds_dict = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds_dict['RDS_HOST']
        USER = creds_dict['RDS_USER']
        PASSWORD = creds_dict['RDS_PASSWORD']
        DATABASE = creds_dict['RDS_DATABASE']
        PORT = creds_dict['RDS_PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()
    def upload_to_db(self, users, table_name):
        sales_engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'postgres'}:{'12345'}@{'localhost'}:{'5432'}/{'sales_data'}")
        users.to_sql(table_name, sales_engine, if_exists='replace')

if __name__ == "__main__":
    connect = DatabaseConnector()
    obj_2 = DataCleaning()
    obj_3 = DataExtractor()
    print(connect.list_db_tables())
    users = obj_3.read_rds_table(connect, 'legacy_users')
    users = obj_2.clean_user_data(users)
    print(connect.upload_to_db(users, 'dim_users'))

