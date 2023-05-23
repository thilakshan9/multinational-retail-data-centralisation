import pandas as pd
class DataCleaning:
    def __init__(self):
        pass
    def clean_user_data(self, users):
        # Checking for any overall null values - none present
        print(f"Total null values in the dataframe are : {users.isna().sum().sum()}")
        # Printing the data types to see if they are of the right type
        print(users.dtypes)
        users['date_of_birth'].to_string('dob_frame.txt')
        users['country'] = users['country'].astype('category')
        users['country_code'] = users['country_code'].astype('category')
        # users['date_of_birth'] = users['date_of_birth'].str.extract(date_pattern, expand=False)
        # users = users[users['date_of_birth'].notnull()]
        # users= users[~users['date_of_birth'].isna()]
        date_of_birth = pd.to_datetime(users['date_of_birth'],format='%Y-%m-%d', errors='coerce')
        date_of_birth= date_of_birth.fillna(pd.to_datetime(users['date_of_birth'], format='%Y/%m/%d', errors='coerce'))
        date_of_birth= date_of_birth.fillna(pd.to_datetime(users['date_of_birth'], format='%B %Y %d', errors='coerce'))
        users['date_of_birth'] = date_of_birth.fillna(pd.to_datetime(users['date_of_birth'], format='%Y %B %d', errors='coerce'))        
        users = users.dropna(subset='date_of_birth')
        # users['date_of_birth'] = pd.to_datetime(users['date_of_birth'], format="%Y-%m-%d", errors='coerce')
        # users['join_date'] = users['join_date'].str.extract(date_pattern, expand=False)
        # users = users[users['join_date'].notnull()]
        # users = users.dropna(subset='join_date')
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
        users.to_string('cleaned_users.txt')
        return users
    def clean_card_data(self, df):
        df.to_string('unclean_card.txt')
        print(f"Total null values in the dataframe are : {df.isna().sum().sum()}")
        df = df[~df.isna()]
        print(df['card_provider'].unique())
        df=df[df['card_provider'].isin(['Diners Club / Carte Blanche','American Express','JCB 16 digit','JCB 15 digit','Maestro', 'Mastercard','Discover','VISA 19 digit', 'VISA 16 digit','VISA 13 digit'])]
        df['card_provider'] = df['card_provider'].astype('category')
        df['card_number'] = df['card_number'].astype(str)
        df['card_number'] = df['card_number'].str.replace('[^a-zA-Z0-9\s]', '', regex=True)
        df.to_string('preclean_card.txt')
        df['card_number'] =  df['card_number'].astype(int)
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df.to_string('clean_card.txt')
        return df
    def called_clean_store_date(self,df):
        print(df['continent'].unique())
        df['continent'] = df['continent'].replace({'eeAmerica':'America', 'eeEurope':'Europe'})
        df=df[df['continent'].isin(['America','Europe'])]
        print(df['continent'].unique())
        df['staff_numbers'] = df['staff_numbers'].str.replace('[a-zA-Z]', '', regex=True)
        df['opening_date'].to_string('date.txt')
        opening_date = pd.to_datetime(df['opening_date'], format='%Y-%m-%d',errors='coerce')
        opening_date= opening_date.fillna(pd.to_datetime(df['opening_date'], format='%B %Y %d', errors='coerce'))
        opening_date= opening_date.fillna(pd.to_datetime(df['opening_date'], format='%Y %B %d', errors='coerce'))
        df['opening_date']= opening_date.fillna(pd.to_datetime(df['opening_date'], format='%Y/%m/%d', errors='coerce'))
        df = df.dropna(subset='opening_date')
        df.to_string('cleaned_stores.txt')
        return df
    def convert_product_weights(self, df):
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
        def convert_value(value):
            if 'x' in str(value) and str(value).endswith('g'):
                parts = str(value).split('x')
                num1 = int(parts[0])
                num2 = int(parts[1][:-1])  # Remove the 'g' character
                return f"{num1 * num2}g"
            else:
                return value
        df['weight'] =  df['weight'].str.replace('[^a-zA-Z0-9]', '', regex=True)
        df['weight'] = df['weight'].apply(convert_value)
        df['weight'] = df['weight'].apply(convert_to_kg)
        df.to_string('cleaned_productdata.csv')
        return df
    def clean_products_data(self, df):
        print(df[df.duplicated(subset=['product_name','weight'], keep=False)])
        date_added= pd.to_datetime(df['date_added'], format='%Y-%m-%d',errors='coerce')
        date_added= date_added.fillna(pd.to_datetime(df['date_added'], format='%Y/%m/%d', errors='coerce'))
        date_added= date_added.fillna(pd.to_datetime(df['date_added'], format='%B %Y %d', errors='coerce'))
        df['date_added']= date_added.fillna(pd.to_datetime(df['date_added'], format='%Y %B %d', errors='coerce'))
        df = df.dropna(subset='date_added')
        df.to_string('cleaned_products.csv')
        return df

    def clean_orders_data(self, df):
        df.to_string('uncleaned_orders.txt')
        df = df.drop(columns=['first_name', 'last_name', '1'])
        df.to_string('cleaned_orders.txt')
        return df
    def clean_dates_data(self,df):
        df.to_string('unclean_dates.csv')
        print(df['month'].isna())
        print(df['year'].isna())
        print(df['day'].isna())
        print(df['time_period'].unique())
        df=df[df['time_period'].isin(['Evening','Morning','Midday','Late_Hours'])]
        print(df['time_period'].unique())
        return df




   
 