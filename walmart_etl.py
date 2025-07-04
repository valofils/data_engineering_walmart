import pandas as pd
from sqlalchemy import create_engine
import os

# Define your directory
DATA_DIR = r"D:\data_engineering\walmart"

# Read CSVs
stores = pd.read_csv(os.path.join(DATA_DIR, 'stores data-set.csv'))
features = pd.read_csv(os.path.join(DATA_DIR, 'Features data set.csv'))
sales = pd.read_csv(os.path.join(DATA_DIR, 'sales data-set.csv'))

# Convert date columns to datetime
features['Date'] = pd.to_datetime(features['Date'], dayfirst=True)
sales['Date'] = pd.to_datetime(sales['Date'], dayfirst=True)

# Create dim_date
dim_date = sales[['Date', 'IsHoliday']].drop_duplicates().copy()
dim_date['year'] = dim_date['Date'].dt.year
dim_date['month'] = dim_date['Date'].dt.month
dim_date['day'] = dim_date['Date'].dt.day
dim_date.reset_index(drop=True, inplace=True)
dim_date['date_id'] = dim_date.index + 1

# Merge date_id back into sales
sales = sales.merge(dim_date, on=['Date', 'IsHoliday'], how='left')

# Create dim_store
dim_store = stores.rename(columns={
    'Store':'store_id',
    'Type':'type',
    'Size':'size'
})

# Create dim_department
dim_department = sales[['Dept']].drop_duplicates().copy()
dim_department['dept_id'] = dim_department.index + 1
dim_department.rename(columns={'Dept':'department_number'}, inplace=True)

# Merge dept_id into sales
sales = sales.merge(dim_department, left_on='Dept', right_on='department_number', how='left')

# Merge features into sales
combined = sales.merge(
    features,
    on=['Store', 'Date', 'IsHoliday'],
    how='left'
)

# Create fact_sales
fact_sales = combined[[
    'date_id',
    'Store',
    'dept_id',
    'Weekly_Sales',
    'Temperature',
    'Fuel_Price',
    'CPI',
    'Unemployment'
]].copy()

fact_sales.rename(columns={
    'Store':'store_id',
    'Weekly_Sales':'weekly_sales',
    'Temperature':'temperature',
    'Fuel_Price':'fuel_price',
    'CPI':'cpi',
    'Unemployment':'unemployment'
}, inplace=True)

# Write to PostgreSQL
engine = create_engine(
    "postgresql+psycopg2://postgres:TayPostGreSQL7%2C@localhost:5432/walmart_db"
)

dim_date[[
    'date_id','Date','year','month','day','IsHoliday'
]].rename(columns={
    'Date':'date',
    'IsHoliday':'is_holiday'
}).to_sql('dim_date', engine, if_exists='append', index=False)

dim_store.to_sql('dim_store', engine, if_exists='append', index=False)

dim_department.to_sql('dim_department', engine, if_exists='append', index=False)

fact_sales.to_sql('fact_sales', engine, if_exists='append', index=False)

print("ETL pipeline finished successfully!")
