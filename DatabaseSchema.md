
# Here I will be listing all of the queries that I ran on my database

### 1. Cast the columns of the orders_table to the correct data types

```sql
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;
ALTER TABLE orders_table
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;
ALTER TABLE orders_table
    ALTER COLUMN card_number TYPE VARCHAR(32);
ALTER TABLE orders_table
    ALTER COLUMN store_code TYPE VARCHAR(16);
ALTER TABLE orders_table
    ALTER COLUMN product_code TYPE VARCHAR(16);
ALTER TABLE orders_table
	ALTER COLUMN product_quantity TYPE SMALLINT;
```
---

### 2. Cast the columns of the dim_users table to the correct data types

```sql
ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255);
ALTER TABLE dim_users
    ALTER COLUMN last_name TYPE VARCHAR(255);
ALTER TABLE dim_users
    ALTER COLUMN date_of_birth TYPE DATE;
ALTER TABLE dim_users
    ALTER COLUMN country_code TYPE VARCHAR(2);
ALTER TABLE dim_users
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;
ALTER TABLE dim_users
	ALTER COLUMN join_date TYPE DATE;
```

---

### 3. Update the dim_store_details table

```sql
ALTER TABLE dim_store_details
    DROP COLUMN lat;

ALTER TABLE dim_store_details
	ADD COLUMN temp_column FLOAT;
UPDATE dim_store_details
	SET temp_column = CASE WHEN longitude = 'N/A' THEN NULL ELSE longitude::FLOAT END;
ALTER TABLE dim_store_details
	DROP COLUMN longitude;
ALTER TABLE dim_store_details
	RENAME COLUMN temp_column to longitude;
ALTER TABLE dim_store_details
    ALTER COLUMN locality TYPE VARCHAR(255);
ALTER TABLE dim_store_details
    ALTER COLUMN store_code TYPE VARCHAR(16);
ALTER TABLE dim_store_details
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint;
ALTER TABLE dim_store_details
    ALTER COLUMN opening_date TYPE DATE;
ALTER TABLE dim_store_details
	ALTER COLUMN store_type TYPE VARCHAR(255);
ALTER TABLE dim_store_details
    ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision;
ALTER TABLE dim_store_details
    ALTER COLUMN country_code TYPE VARCHAR(2);
ALTER TABLE dim_store_details
    ALTER COLUMN continent TYPE VARCHAR(255);
```

---

### 4. Make changes to the dim_products table for the delivery team

```sql
UPDATE dim_products
SET weight_class = CASE
           WHEN CAST(weight AS FLOAT) < 2 THEN 'Light'
           WHEN CAST(weight AS FLOAT) BETWEEN 2 AND 40 THEN 'Mid-Sized'
           WHEN CAST(weight AS FLOAT) BETWEEN 40 AND 140 THEN 'Heavy'
           ELSE 'Truck_Required'
       END 

ALTER TABLE dim_products
RENAME removed TO still_avaliable
UPDATE dim_products
SET still_avaliable = CASE
                                  WHEN still_avaliable = 'Still_avaliable' THEN TRUE
                                  WHEN still_avaliable = 'Removed' THEN FALSE
                              END;

```

---

### 5. Update the dim_products table with the requried data types

```sql
ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision;
ALTER TABLE dim_products
    ALTER COLUMN weight TYPE FLOAT;
ALTER TABLE dim_products
	ALTER COLUMN "EAN" TYPE VARCHAR(20);
ALTER TABLE dim_products
    ALTER COLUMN product_code TYPE VARCHAR(16);
ALTER TABLE dim_products
    ALTER COLUMN date_added TYPE DATE;
ALTER TABLE dim_products
    ALTER COLUMN uuid TYPE UUID USING uuid::uuid;
ALTER TABLE dim_products
    ALTER COLUMN still_avaliable TYPE BOOL USING still_avaliable::boolean;
ALTER TABLE dim_products
    ALTER COLUMN weight_class TYPE VARCHAR(16);
ALTER TABLE dim_products
DROP COLUMN "Unnamed: 0”;
```

---

### 6. Update the dim_date_times table

```sql
ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(2);
ALTER TABLE dim_date_times
    ALTER COLUMN year TYPE VARCHAR(4);
ALTER TABLE dim_date_times
    ALTER COLUMN day TYPE VARCHAR(2);
ALTER TABLE dim_date_times
    ALTER COLUMN time_period TYPE VARCHAR(10);
ALTER TABLE dim_date_times
    ALTER COLUMN DATE_UUID TYPE UUID USING date_uuid::uuid;
```

---

### 7. Update the dim_card_details table

```sql
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(19);
ALTER TABLE dim_card_details
    ALTER COLUMN expiry_date TYPE VARCHAR(5);
ALTER TABLE dim_card_details
    ALTER COLUMN date_payment_confirmed TYPE DATE;
```
---


### 8. Create the primary keys in the dimension tables

```sql
ALTER TABLE dim_products
ADD CONSTRAINT pk_dim_prodcuts PRIMARY KEY (product_code)

ALTER TABLE dim_date_times
ADD CONSTRAINT pk_dim_date_times PRIMARY KEY (date_uuid)

ALTER TABLE dim_card_details
ADD CONSTRAINT pk_dim_card_details PRIMARY KEY (card_number)

ALTER TABLE dim_users
ADD CONSTRAINT pk_dim_users PRIMARY KEY (user_uuid);

ALTER TABLE dim_store_details
ADD CONSTRAINT pk_store_card_details PRIMARY KEY (store_code)
```

---

### 9. Finalise the star-based schema and add the foreign keys to the orders table

```sql
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_user FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid)

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_card FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number)

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_store FOREIGN KEY (store_code) REFERENCES dim_store_detials (store_code)

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_product FOREIGN KEY (product_code) REFERENCES dim_products (product_code)

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_date_time FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid)
```
