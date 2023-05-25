import pandas as pd
class DataCleaning:
    """
        This class is used to clean data from each of the data sources

        Methods:
        clean_user_data - This method willl perform the cleaning of the user data. Look out for NULL values, errors with dates, 
        incorrectly typed values and rows filled with the wrong information
        clean_card_data - This method is used to clean the datas to remove any erroneous values, NULL values or errors with formatting
        called_clean_store_data - This method is used to clean the data retrieve from the API and returns a pandas dataframe
        convert_product_weights - This method is used to convert all the weights to the same measurement in kg
        clean_products_data - This method is used to clean the dataframe of any additional erroneous values
        clean_orders_data - This method cleans the orders table by removing the first_name, last_name and 1 column
        clean_dates_data - This method cleans the dates table
    """
    def __init__(self):
        pass
    def clean_user_data(self, users):
        # Checking for any overall null values - none present
        print(f"Total null values in the dataframe are : {users.isna().sum().sum()}")
        # Printing the data types to see if they are of the right type
        print(users.dtypes)
        users['country'] = users['country'].astype('category')
        users['country_code'] = users['country_code'].astype('category')
        # Converting date to the right format for all the different formats and dropping na values
        date_of_birth = pd.to_datetime(users['date_of_birth'],format='%Y-%m-%d', errors='coerce')
        date_of_birth= date_of_birth.fillna(pd.to_datetime(users['date_of_birth'], format='%Y/%m/%d', errors='coerce'))
        date_of_birth= date_of_birth.fillna(pd.to_datetime(users['date_of_birth'], format='%B %Y %d', errors='coerce'))
        users['date_of_birth'] = date_of_birth.fillna(pd.to_datetime(users['date_of_birth'], format='%Y %B %d', errors='coerce'))        
        users = users.dropna(subset='date_of_birth')
        # Converting date to the right format for all the different formats and dropping na values
        join_date = pd.to_datetime(users['join_date'],format='%Y-%m-%d', errors='coerce')
        join_date= join_date.fillna(pd.to_datetime(users['join_date'], format='%Y/%m/%d', errors='coerce'))
        join_date= join_date.fillna(pd.to_datetime(users['join_date'], format='%B %Y %d', errors='coerce'))
        users['join_date'] = join_date.fillna(pd.to_datetime(users['join_date'], format='%Y %B %d', errors='coerce'))
        users = users.dropna(subset='join_date')
        users = users.reset_index(drop=True)
        # Replacing GGB with GB - inferring
        users['country_code'] = users['country_code'].replace({'GGB':'GB'})
        users['address'] = users['address'].str.replace("\n"," ").str.replace("\\","").str.replace("/","")
        # Used the following to get the domain name extensions
        domain = users['email_address'].apply(lambda x: x.split('@')[-1])
        domain = domain.apply(lambda x: x.split('.',1)[-1])
        # Emails must contain @ and domain name extension
        users = users[users['email_address'].str.contains('@')]
        users = users[users['email_address'].str.contains('.co.uk|.com|.de|.org|.info|.net|.biz')]
        users = users.reset_index(drop=True)
        return users
    def clean_card_data(self, df):
        # Checking for any overall null values 
        print(f"Total null values in the dataframe are : {df.isna().sum().sum()}")
        # Removing null values 
        df = df[~df.isna()]
        # Checking all the unique category values
        print(df['card_provider'].unique())
        # Removing any unique category values that are erroneous
        df=df[df['card_provider'].isin(['Diners Club / Carte Blanche','American Express','JCB 16 digit','JCB 15 digit','Maestro', 'Mastercard','Discover','VISA 19 digit', 'VISA 16 digit','VISA 13 digit'])]
        df['card_provider'] = df['card_provider'].astype('category')
        df['card_number'] = df['card_number'].astype(str)
        # Removing symbols from card_number
        df['card_number'] = df['card_number'].str.replace('[^a-zA-Z0-9\s]', '', regex=True)
        df['card_number'] =  df['card_number'].astype(int)
        # Converting date to right format
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        return df
    def called_clean_store_date(self,df):
        # Checking all the unique continents
        print(df['continent'].unique())
        # Inferring some continent values
        df['continent'] = df['continent'].replace({'eeAmerica':'America', 'eeEurope':'Europe'})
        # Removing any unique continent values that are erroneous 
        df=df[df['continent'].isin(['America','Europe'])]
        print(df['continent'].unique())
        # Making sure staff_numbers has only numbers
        df['staff_numbers'] = df['staff_numbers'].str.replace('[a-zA-Z]', '', regex=True)
        df['opening_date'].to_string('date.txt')
        # Converting date to the right format for all the different formats and dropping na values
        opening_date = pd.to_datetime(df['opening_date'], format='%Y-%m-%d',errors='coerce')
        opening_date= opening_date.fillna(pd.to_datetime(df['opening_date'], format='%B %Y %d', errors='coerce'))
        opening_date= opening_date.fillna(pd.to_datetime(df['opening_date'], format='%Y %B %d', errors='coerce'))
        df['opening_date']= opening_date.fillna(pd.to_datetime(df['opening_date'], format='%Y/%m/%d', errors='coerce'))
        df = df.dropna(subset='opening_date')
        return df
    def convert_product_weights(self, df):
        # This function converts all the units of measure to kg
        def convert_to_kg(value):
            try:
                if 'kg' in str(value):
                    kilograms = float(str(value).replace('kg', ''))
                    return kilograms
                elif 'g' in str(value):
                    grams = float(str(value).replace('g', ''))
                    kilograms = grams / 1000
                    return kilograms
                elif 'ml' in str(value):
                    milliliters = float(str(value).replace('ml', ''))
                    kilograms = milliliters / 1000
                    return kilograms
                elif 'oz' in str(value):
                    ounces = float(str(value).replace('oz',''))
                    kilograms = ounces * 0.0283495
                    return kilograms 
                else:
                    return value
            except ValueError:
                return value
        # This function converts any multi-pack grams into single gram measurement
        def convert_value(value):
            if 'x' in str(value) and str(value).endswith('g'):
                parts = str(value).split('x')
                num1 = int(parts[0])
                num2 = int(parts[1][:-1])  # Remove the 'g' character
                return f"{num1 * num2}g"
            else:
                return value
        # Makes sure that the weight are all in kg and they are no symbols
        df['weight'] =  df['weight'].str.replace('[^a-zA-Z0-9]', '', regex=True)
        df['weight'] = df['weight'].apply(convert_value)
        df['weight'] = df['weight'].apply(convert_to_kg)
        return df
    
    def clean_products_data(self, df):
        # Looking for any duplicated values
        print(df[df.duplicated(subset=['product_name','weight'], keep=False)])
        # Converting date to the right format for all the different formats and dropping na values
        date_added= pd.to_datetime(df['date_added'], format='%Y-%m-%d',errors='coerce')
        date_added= date_added.fillna(pd.to_datetime(df['date_added'], format='%Y/%m/%d', errors='coerce'))
        date_added= date_added.fillna(pd.to_datetime(df['date_added'], format='%B %Y %d', errors='coerce'))
        df['date_added']= date_added.fillna(pd.to_datetime(df['date_added'], format='%Y %B %d', errors='coerce'))
        df = df.dropna(subset='date_added')
        return df

    def clean_orders_data(self, df):
        # Drops the first_name, last_name and 1 column
        df = df.drop(columns=['first_name', 'last_name', '1'])
        return df
    
    def clean_dates_data(self,df):
        # Checking all the unique time_period values
        print(df['time_period'].unique())
        # Removing any unique time_period values that are erroneous
        df=df[df['time_period'].isin(['Evening','Morning','Midday','Late_Hours'])]
        return df




   
 