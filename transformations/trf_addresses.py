import pandas as pd
from sqlalchemy import create_engine
from credentials import db_config, db_config_silver

# Establishing a connection to the PostgreSQL database
connection_string = (f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
                     f"@{db_config['host']}:{db_config['port']}/{db_config['database']}")
engine = create_engine(connection_string)

# Read data from the addresses table of the Bronze layer
df_addresses = pd.read_sql_table('addresses', engine, schema='csv')

# Rename column address_id to id_address and reorganise the columns
df_addresses.rename(columns={'address_id': 'id_address'}, inplace=True)
df_addresses = df_addresses[['id_address', 'country', 'state', 'address', 'zipcode', 'load_date']]

# Caste the data to a maximum of 75 characters
df_addresses['country'] = df_addresses['country'].astype(str).str[:75]
df_addresses['state'] = df_addresses['state'].astype(str).str[:75]
df_addresses['address'] = df_addresses['address'].astype(str).str[:75]

# Show the first rows to verify the changes
print(df_addresses.head())

connection_string_silver = (f"postgresql+psycopg2://{db_config_silver['user']}:{db_config_silver['password']}"
                            f"@{db_config_silver['host']}:{db_config_silver['port']}/{db_config_silver['database']}")

engine_silver = create_engine(connection_string_silver)

# Load the transformed data into a new table in the Silver zone
df_addresses.to_sql('trf_addresses', engine_silver, if_exists='replace', index=False, schema='csv')
