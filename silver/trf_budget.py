import pandas as pd
from sqlalchemy import create_engine
from credentials import db_config, db_config_silver

# Establish a connection to the Bronze PostgreSQL database
bronze_engine = create_engine(
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
    f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# Read data from the budget table of the Bronze layer
df_budget = pd.read_sql_table('budget', bronze_engine, schema='csv')

# Rename column product_id to id_product and reorganise the columns
df_budget.rename(columns={'product_id': 'id_product'}, inplace=True)
df_budget = df_budget[['_row', 'id_product', 'quantity', 'month', 'load_date']]

# Cast the id_product to a maximum of 75 characters
df_budget['id_product'] = df_budget['id_product'].astype(str).str[:75]

# Convert the month column to UTC and rename it to month_utc
df_budget['month'] = pd.to_datetime(df_budget['month']).dt.tz_localize('CET').dt.tz_convert('UTC')
df_budget.rename(columns={'month': 'month_utc'}, inplace=True)

# Show the first rows to verify the changes
pd.set_option('display.max_columns', None)  # Displays all columns
pd.set_option('display.expand_frame_repr', False)  # Avoid multiple line representation
print(df_budget.head())

# Establish a connection to the Silver PostgreSQL database
silver_engine = create_engine(
    f"postgresql+psycopg2://{db_config_silver['user']}:{db_config_silver['password']}@"
    f"{db_config_silver['host']}:{db_config_silver['port']}/{db_config_silver['database']}"
)

# Load the transformed data into a new table in the Silver zone
df_budget.to_sql('trf_budget', silver_engine, if_exists='replace', index=False, schema='csv')
