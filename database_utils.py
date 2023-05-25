import yaml
from sqlalchemy import create_engine, inspect
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

class DatabaseConnector:
    """
    This class is used to econnect with and upload data to the database

    Methods:
    read_db_creds - This methods reads the credentials yaml file and returns a dictionary of the credentials
    init_db_engine - This methods reads the credentials from the return of read_db_creds and initliase and return 
    an sqlachemy database engine
    list_db_tables - Uses the engine to list all the tables in the database so you know which tables you can extract
    data from
    upload_to_db - This method will take in a pandas dataframe as an argument and upload to as an argument
    
    """
    def __init__(self):
        pass
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as f:
            # This reads the db_creds.yaml and saves the dictionary of the credentials to the creds_dict variable
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
        # Creates an engine which contains information about type of database and connection pool - aws database
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        # Displays all the table names
        return inspector.get_table_names()
    def upload_to_db(self, df, table_name):
        # Creates an engine for the local database that we created
        sales_engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'postgres'}:{'12345'}@{'localhost'}:{'5432'}/{'sales_data'}")
        # Uploads to the local database
        df.to_sql(table_name, sales_engine, if_exists='replace')

if __name__ == "__main__":
    # Instantiates the objects 
    connect = DatabaseConnector()
    obj_2 = DataCleaning()
    obj_3 = DataExtractor()
    # Retrieves and cleans the data for the given data and uploads them to the database
    card_data  = obj_3.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    card_data = obj_2.clean_card_data(card_data)
    connect.upload_to_db(card_data, 'dim_card_details')
    stores_data = obj_3.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/%7Bstore_number%7D')
    stores_data = obj_2.called_clean_store_date(stores_data)
    connect.upload_to_db(stores_data, 'dim_store_details')
    users = obj_3.read_rds_table(connect, 'legacy_users')
    users = obj_2.clean_user_data(users)   
    connect.upload_to_db(users, 'dim_users')
    product_data = obj_3.extractfroms3()
    product_data = obj_2.convert_product_weights(product_data)
    data = obj_2.clean_products_data(product_data)
    connect.upload_to_db(data, 'dim_products')
    orders_data = obj_3.read_rds_table(connect, 'orders_table')
    orders_data = obj_2.clean_orders_data(orders_data)
    connect.upload_to_db(orders_data, 'orders_table')
    date_data = obj_3.extract_dates()
    date_data = obj_2.clean_dates_data(date_data)
    connect.upload_to_db(date_data, 'dim_date_times')



