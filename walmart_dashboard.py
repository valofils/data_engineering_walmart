# database connection
from sqlalchemy import create_engine
import pandas as pd

# Your DB connection string
db_url = "postgresql+psycopg2://postgres:TayPostGreSQL7%2C@localhost:5432/walmart_db"

engine = create_engine(db_url)




# Total sales by store
query_sales_by_store = """
SELECT
    s.store_id,
    s.type,
    SUM(f.weekly_sales) AS total_sales
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY s.store_id, s.type
ORDER BY total_sales DESC;
"""

df_sales_by_store = pd.read_sql(query_sales_by_store, engine)





# Sales over time
query_sales_over_time = """
SELECT
    d.date,
    SUM(f.weekly_sales) AS total_sales
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.date
ORDER BY d.date;
"""

df_sales_over_time = pd.read_sql(query_sales_over_time, engine)



# streamlit

import streamlit as st
import matplotlib.pyplot as plt

st.set_page_config(page_title="Walmart Sales Dashboard", layout="wide")

st.title("🛒 Walmart Sales Dashboard")

st.subheader("Total Sales by Store")

st.dataframe(df_sales_by_store)



# Filter by Store Type
store_types = df_sales_by_store["type"].unique()
selected_type = st.selectbox("Select Store Type", store_types)

filtered_df = df_sales_by_store[df_sales_by_store["type"] == selected_type]

st.subheader(f"Total Sales for Store Type {selected_type}")
st.bar_chart(filtered_df.set_index("store_id")["total_sales"])




# Plot sales by store
fig, ax = plt.subplots()
ax.bar(df_sales_by_store["store_id"], df_sales_by_store["total_sales"])
ax.set_xlabel("Store ID")
ax.set_ylabel("Total Sales")
ax.set_title("Total Sales by Store")
st.pyplot(fig)

# Sales over time
st.subheader("Sales Over Time")

st.line_chart(df_sales_over_time.set_index("date")["total_sales"])


# Sales Over Time for One Store
# Fetch all store IDs
store_ids = df_sales_by_store["store_id"].unique()
selected_store = st.selectbox("Select Store ID", store_ids)

query_store_trend = f"""
SELECT
    d.date,
    SUM(f.weekly_sales) AS total_sales
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
WHERE f.store_id = {selected_store}
GROUP BY d.date
ORDER BY d.date;
"""

df_store_trend = pd.read_sql(query_store_trend, engine)

st.subheader(f"Sales Trend for Store {selected_store}")
st.line_chart(df_store_trend.set_index("date")["total_sales"])


# Top Departments by Sales
query_dept = """
SELECT
    d.department_number,
    SUM(f.weekly_sales) AS total_sales
FROM fact_sales f
JOIN dim_department d ON f.dept_id = d.dept_id
GROUP BY d.department_number
ORDER BY total_sales DESC
LIMIT 10;
"""

df_dept = pd.read_sql(query_dept, engine)

st.subheader("Top 10 Departments by Sales")
st.dataframe(df_dept)

import matplotlib.pyplot as plt

fig, ax = plt.subplots()
ax.pie(df_dept["total_sales"], labels=df_dept["department_number"], autopct="%1.1f%%")
st.pyplot(fig)


#  Fuel Price vs Sales Correlation
query_fuel = """
SELECT
    ROUND(f.fuel_price::numeric, 2) AS fuel_price,
    SUM(f.weekly_sales) AS total_sales
FROM fact_sales f
GROUP BY ROUND(f.fuel_price::numeric, 2)
ORDER BY fuel_price;
"""

df_fuel = pd.read_sql(query_fuel, engine)

st.subheader("Sales vs Fuel Price")

st.bar_chart(df_fuel.set_index("fuel_price")["total_sales"])
