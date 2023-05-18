import yaml
from sqlalchemy import create_engine, inspect

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

obj_1 = DatabaseConnector()
print(obj_1.list_db_tables())

