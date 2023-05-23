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
    def upload_to_db(self, df, table_name):
        sales_engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'postgres'}:{'12345'}@{'localhost'}:{'5432'}/{'sales_data'}")
        df.to_sql(table_name, sales_engine, if_exists='replace')

if __name__ == "__main__":
    connect = DatabaseConnector()
    obj_2 = DataCleaning()
    obj_3 = DataExtractor()
    card  = obj_3.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    card = obj_2.clean_card_data(card)
    connect.upload_to_db(card, 'dim_card_details')
    stores_data = obj_3.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/%7Bstore_number%7D')
    stores_data = obj_2.called_clean_store_date(stores_data)
    connect.upload_to_db(stores_data, 'dim_store_details')
    print(connect.list_db_tables())
    users = obj_3.read_rds_table(connect, 'legacy_users')
    users = obj_2.clean_user_data(users)   
    connect.upload_to_db(users, 'dim_users')
    print(connect.list_db_tables())
    product_data = obj_3.extractfroms3()
    product_data = obj_2.convert_product_weights(product_data)
    data = obj_2.clean_products_data(product_data)
    connect.upload_to_db(data, 'dim_products')
    orders_data = obj_3.read_rds_table(connect, 'orders_table')
    orders_data = obj_2.clean_orders_data(orders_data)
    connect.upload_to_db(orders_data, 'orders_table')
    json_data = obj_3.extract_dates()
    json_data = obj_2.clean_dates_data(json_data)
    connect.upload_to_db(json_data, 'dim_date_times')



