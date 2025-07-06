import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

USERNAME, PASSWORD, DATABASE, PORT, HOST = "add_yours" 

# Extract data
df = pd.read_csv("data/supermarket_sales.csv")

# connect to default 'postgres' database to create a new one
engine = create_engine(f'postgresql+psycopg2://{USERNAME}:{PASSWORD}@localhost:5432/postgres')

# create the database
try:
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT").execute(
            text(f"CREATE DATABASE {DATABASE};")
        )
    
    print("Database Created Successfully.")
except Exception as e:
    print("Database could not be created. Reason: ", e)


# Create Connection
conn = psycopg2.connect(
    dbname = DATABASE,
    user = USERNAME,
    password = PASSWORD,
    host = HOST,
    port = PORT
)

cursor = conn.cursor()

# create table sales 
create_table_query = '''
CREATE TABLE IF NOT EXISTS sales (
    invoice_id TEXT PRIMARY KEY,
    branch TEXT,
    customer_type TEXT,
    gender TEXT,
    product_line TEXT,
    unit_price NUMERIC(10,2),
    quantity INT,
    total NUMERIC(10,2),
    date DATE,
    payment TEXT
);
'''

cursor.execute(create_table_query)
conn.commit()
cursor.close()
conn.close()

print("Table sales is now available.")

# df.info()

# -------------Transform data---------

# select only the required columns

# df_selected = df.drop(columns=['cogs', 'Rating', 'gross income', 'gross margin percentage', 'Time', 'Tax 5%', 'City'])

# -- Prefered way
df_selected = df[['Invoice ID', 'Branch', 'Customer type','Gender', 'Product line','Unit price', 'Quantity', 'Total', 'Date', 'Payment']]

# convert 'Date' to datetime format
df_selected.loc[:, 'Date'] = pd.to_datetime(df_selected['Date'], format='%m/%d/%Y')

# Rename columns to match SQLcolumn names
df_selected.columns = ['invoice_id', 'branch', 'customer_type', 'gender', 'product_line',
                       'unit_price', 'quantity', 'total', 'date', 'payment']


# ---------------load into PostgreSQL-------------
connection_string = f'postgresql+psycopg2://{USERNAME}:{PASSWORD}@localhost:5432/{DATABASE}'
engine = create_engine(connection_string)

df_selected.to_sql('sales', con=engine, if_exists='append', index=False)

print("ETL complete: Data loaded into PostgreSQL.")