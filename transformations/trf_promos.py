import pandas as pd
from sqlalchemy import create_engine
import hashlib
from datetime import datetime
from credentials import db_config, db_config_silver

# Establish a connection to the Bronze PostgreSQL database
bronze_engine = create_engine(
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
    f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# Read data from the promos table of the Bronze layer
df_promos = pd.read_sql_table('promos', bronze_engine, schema='csv')

# Renaming promo_id to promo_name
df_promos.rename(columns={'promo_id': 'promo_name'}, inplace=True)

# Casting varchar fields
df_promos['promo_name'] = df_promos['promo_name'].astype(str).str[:75]
df_promos['status'] = df_promos['status'].astype(str).str[:75]

# Capitalizing each word in promo_name
df_promos['promo_name'] = df_promos['promo_name'].str.title()

# Add a new row for the 'Without Promotion'
new_promo = {
    'promo_name': 'Without Promotion',
    'discount': 0,
    'status': 'Inactive',
    'load_date': datetime.strptime("2023-12-18 19:34:45.988818", "%Y-%m-%d %H:%M:%S.%f")
}

# Append the new row to the DataFrame
new_promo_df = pd.DataFrame([new_promo])
df_promos = pd.concat([df_promos, new_promo_df], ignore_index=True)

# Generating a surrogate key (SK) for id_promo using a hash function
df_promos['id_promo'] = [hashlib.sha256(str.encode(promo_name)).hexdigest() for promo_name in df_promos['promo_name']]

# Reorganizing the columns to put id_promo first
df_promos = df_promos[['id_promo', 'promo_name', 'discount', 'status', 'load_date']]

# Show the first rows to verify the changes
print(df_promos.head())

# Establishing a connection to the Silver PostgreSQL database
engine_silver = create_engine(
    f"postgresql+psycopg2://{db_config_silver['user']}:{db_config_silver['password']}@"
    f"{db_config_silver['host']}:{db_config_silver['port']}/{db_config_silver['database']}"
)

# Load the transformed data into a new table in the Silver zone
df_promos.to_sql('trf_promos', engine_silver, if_exists='replace', index=False, schema='csv')
