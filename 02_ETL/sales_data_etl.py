# -- Importing Libraries -- #

print('\n')
print('Importing libraries to perform ETL...')

import pandas as pd
import sqlite3
import pyfiglet
import warnings
warnings.filterwarnings('ignore')

print('Initiating ETL Process...')
print('\n')

# -- Starting ETL Process --#

etl_title = "SALES DATA ETL"
ascii_art_title = pyfiglet.figlet_format(etl_title, font='small')
print(ascii_art_title)
print('\n')

# -- Connecting to Dataset -- #

print('Connecting to source dataset')

dim_customers = pd.read_csv("../01_SOURCE/Dim_Customers.csv", index_col=None)
dim_products = pd.read_csv("../01_SOURCE/Dim_Products.csv", index_col=None)
dim_regions = pd.read_csv('../01_SOURCE/Dim_Regions.csv', index_col=None)
dim_store_locations = pd.read_csv("../01_SOURCE/Dim_StoreLocations.csv", index_col=None)
fact_orders = pd.read_csv("../01_SOURCE/Fact_SalesOrders.csv", index_col=None)

print('\n')

# -- Transforming "dim_customers" data --#

print('Transforming "dim_customers" data')
print(f'Shape of "dim_customers" dataset: {dim_customers.shape}')
print(f'Columns in "dim_customers" dataset: {list(dim_customers.columns)}')
print('\n')


# -- Renaming Existing Columns --#

print('Renaming existing columns in "dim_customers" dataset')

customers_new_col_names = ['CustomerID', 'Customer Name']
dim_customers.columns = customers_new_col_names

print(f'New column names in the "dim_customers" dataset: {list(dim_customers.columns)}')
print('\n')

# -- Transforming "dim_products" data --#

print('Transforming "dim_products" data')
print(f'Shape of "dim_products" dataset: {dim_products.shape}')
print(f'Columns in "dim_products" dataset: {list(dim_products.columns)}')
print('\n')

# -- Renaming Existing Columns --#

print('Renaming existing columns in "dim_products" dataset')

products_new_col_names = ['ProductID', 'Product Name']
dim_products.columns = products_new_col_names

print(f'New column names in the "dim_products" dataset: {list(dim_products.columns)}')
print('\n')

# -- Transforming "dim_regions" & "dim_store_locations" data --#

print(f'Shape of store locations dataset: {dim_store_locations.shape}')
print(f'Shape of state regions dataset: {dim_regions.shape}')
print('\n')

# -- Joining 'store locations' & 'state regions' Tables -- #

print('Joining "store locations" & "state regions" Tables')

merged_data = pd.merge(dim_store_locations, dim_regions, how='left', on='StateCode')

print(f'Shape of merged dataset: {merged_data.shape}')
print(f'Columns in merged dataset: {list(merged_data.columns)}')
print('\n')

# -- Removing Unnecessary Columns --#

print('Removing unnecessary columns')

stores_keep_columns = ['_StoreID', 'Region', 'State_x', 'City Name', 'Latitude', 'Longitude']
dim_stores = merged_data[stores_keep_columns]

print(f'Shape of dataframe after removal of unnecessary columns: {dim_stores.shape}')
print('\n')

# -- Renaming Existing Columns --#

print('Renaming existing columns')

stores_new_column_names = ['StoreID', 'Region', 'State', 'City', 'Latitude', 'Longitude']
dim_stores.columns = stores_new_column_names

print(f'New column names in the dataframe: {list(dim_stores.columns)}')
print('\n')

# -- Transforming "fact_orders" data --#

print('Transforming "fact_orders" data')
print(f'Shape of "fact_orders" dataset: {fact_orders.shape}')
print(f'Columns in "fact_orders" dataset: {list(fact_orders.columns)}')
print('\n')

# -- Removing Unnecessary Columns --#

print('Removing unnecessary columns')

orders_keep_columns = ['OrderNumber', '_CustomerID', '_ProductID', '_StoreID', 'Sales Channel', 'OrderDate', 'Order Quantity','Discount Applied', 'Unit Price', 'Unit Cost']

fact_orders = fact_orders[orders_keep_columns]

print(f'Shape of dataframe after removal of unnecessary columns: {fact_orders.shape}')
print('\n')

# -- Renaming Existing Columns --#

print('Renaming existing columns')

orders_new_column_names = ['OrderID', 'CustomerID', 'ProductID', 'StoreID', 'Sales Channel', 'OrderDate', 'Order Quantity','Discount Applied', 'Unit Price', 'Unit Cost']
fact_orders.columns = orders_new_column_names

print(f'New column names in the dataframe: {list(fact_orders.columns)}')
print('\n')

# -- Exporting Data to CSV File --#

print('Exporting the dataframes to CSV files...')

dim_customers.to_csv('../03_DATA/FLATFILES/Dim_Customers.csv', encoding='utf-8', index=False)
dim_products.to_csv('../03_DATA/FLATFILES/Dim_Products.csv', encoding='utf-8', index=False)
dim_stores.to_csv('../03_DATA/FLATFILES/Dim_Stores.csv', encoding='utf-8', index=False)
fact_orders.to_csv('../03_DATA/FLATFILES/Fact_SalesOrders.csv', encoding='utf-8', index=False)

print('Data exported to CSV...')
print('\n')


# -- SQLite Database -- #
# ===================== #

# -- Connecting to Database --#

print('Connecting to Database')

connection = sqlite3.connect('../03_DATA/DATABASE/US_Regional_Sales.db')
cursor = connection.cursor()

print('\n')

# -- Inserting Data into Database Tables --#

print('Inserting data into database tables')

dim_customers.to_sql('dim_customers', connection, if_exists='replace', index=False)
dim_products.to_sql('dim_products', connection, if_exists='replace', index=False)
dim_stores.to_sql('dim_stores', connection, if_exists='replace', index=False)
fact_orders.to_sql('fact_orders', connection, if_exists='replace', index=False)

connection.commit()

print('Data added to the database successfully')
print('\n')

# -- Closing the database --#

connection.commit()

print('Database connected closed')
print('\n')
print('ETL Process completed !!!')