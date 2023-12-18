import pandas as pd
from sqlalchemy import create_engine
from credentials import db_config
import datetime


# Function to load a CSV file into the corresponding PostgreSQL table
def load_csv_to_postgresql(file_name, table_name, engine, schema='csv'):
    df = pd.read_csv(f'Data/{file_name}.csv', delimiter=';')
    # Convert all columns to lower case
    df.columns = [col.lower() for col in df.columns]
    # Adds the load date column to the DataFrame
    df['load_date'] = datetime.datetime.now()
    # Load the data into the PostgreSQL table
    df.to_sql(table_name, engine, if_exists='append', index=False, schema=schema)
    print(f"Data loaded into table {schema}.{table_name}")


# Establish a connection to the PostgreSQL database
connection_string = (f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
                     f"@{db_config['host']}:{db_config['port']}/{db_config['database']}")
engine = create_engine(connection_string)

# Dictionary of file names and corresponding table names to load
files_and_tables = {
    "Tabla_addresses": "addresses",
    "Tabla_budget": "budget",
    "Tabla_events": "events",
    "Tabla_order_items": "order_items",
    "Tabla_orders": "orders",
    "Tabla_products": "products",
    "Tabla_promos": "promos",
    "Tabla_users": "users"
}

# Iterate over the dictionary and load each CSV file into the corresponding table
for file_name, table_name in files_and_tables.items():
    load_csv_to_postgresql(file_name, table_name, engine)
