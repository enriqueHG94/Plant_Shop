import pandas as pd
from sqlalchemy import create_engine
from credentials import db_config_silver, db_config_gold

# Establish a connection to the silver PostgreSQL database
silver_engine = create_engine(
    f"postgresql+psycopg2://{db_config_silver['user']}:{db_config_silver['password']}@"
    f"{db_config_silver['host']}:{db_config_silver['port']}/{db_config_silver['database']}"
)

# Read data from the promos table of the silver zone
dim_promos = pd.read_sql_table('trf_promos', silver_engine, schema='csv')

# Show the first rows to verify the changes
pd.set_option('display.max_columns', None)  # Displays all columns
pd.set_option('display.expand_frame_repr', False)  # Avoids line break in the console output
print(dim_promos.head())

# Establish a connection to the gold PostgreSQL database
gold_engine = create_engine(
    f"postgresql+psycopg2://{db_config_gold['user']}:{db_config_gold['password']}@"
    f"{db_config_gold['host']}:{db_config_gold['port']}/{db_config_gold['database']}"
)

# Load the transformed data into a new table in the gold zone
dim_promos.to_sql('dim_promos', gold_engine, if_exists='replace', index=False, schema='csv')
