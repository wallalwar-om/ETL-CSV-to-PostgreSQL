import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from getpass import getpass


def create_db(credentials):
    default_database_name = input('Default_db: ')

    # connect to default database to create a new one
    engine = create_engine(f"postgresql+psycopg2://{credentials['user']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{default_database_name}")

    # create the database
    try:
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT").execute(
                text(f"CREATE DATABASE {credentials['dbname']};")
            )
    
        print("Database Created Successfully.")
    except Exception as e:
        print("Database could not be created. Reason: ", e)


def connect_to_db(credentials):

    # Create Connection
    connection = psycopg2.connect(
        user = credentials['user'],
        password = credentials['password'],
        host = credentials['host'],
        port = credentials['port'],
        dbname = credentials['dbname']
    )

    return connection

def cursor_to_db(connection):
    return connection.cursor()


def commit_db(connection):
    connection.commit()


def execute_query(cursor, query):
    cursor.execute(query)


def close_cursor(cursor):
    cursor.close()


def close_db_connection(connection):
    connection.close()


def create_table_to_db(cursor, connection):

    TABLE_NAME = input("Table name to create table: ")

    # create table sales 
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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

    execute_query(cursor, create_table_query)
    commit_db(connection)

    print(f"Table {TABLE_NAME} is now available.")

    return TABLE_NAME

def transform_df(df):
    # -------------Transform data---------
    # select only the required columns
    # df_selected = df.drop(columns=['cogs', 'Rating', 'gross income', 'gross margin percentage', 'Time', 'Tax 5%', 'City'])

    # -- Prefered way
    df_selected = df[['Invoice ID', 'Branch', 'Customer type','Gender', 'Product line','Unit price', 'Quantity', 'Total', 'Date', 'Payment']]

    # convert 'Date' to datetime format
    df_selected.loc[:, 'Date'] = pd.to_datetime(df_selected['Date'], format='%m/%d/%Y')

    # Rename columns to match SQLcolumn names
    df_selected.columns = ['invoice_id', 'branch', 'customer_type', 'gender', 'product_line','unit_price', 'quantity', 'total', 'date', 'payment']

    return df_selected

def load_to_postgresql(table, df, credentials):
    connection_string = f"postgresql+psycopg2://{credentials['user']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['dbname']}"
    engine = create_engine(connection_string)
    df.to_sql(table, con=engine, if_exists='append', index=False)

    print("ETL complete: Data loaded into PostgreSQL.")

def main():

    FILE_PATH = input("Enter path to Dataset: ")
    USERNAME = input('Username: ')
    PASSWORD = getpass("Password: ")
    DATABASE = input('DB_Name: ')
    PORT = 5432
    HOST = input("Host (default=localhost): ") or "localhost"

    credentials = {
        "user": USERNAME,
        "password": PASSWORD,
        "host": HOST,
        "port": PORT,
        "dbname": DATABASE
    }

    # Extract data
    df = pd.read_csv(FILE_PATH)

    create_db(credentials)

    try:
        conn = connect_to_db(credentials)
    except Exception as e:
        print("Could not connect to DB:", e)
        return
    
    cursor = cursor_to_db(conn)
    table = create_table_to_db(cursor, conn)
    close_cursor(cursor)
    close_db_connection(conn)
    transformed_df = transform_df(df)

    try:
        load_to_postgresql(table, transformed_df, credentials)
    except Exception as e:
        print("Failed to load data:", e)
    

if __name__ == "__main__":
    main()