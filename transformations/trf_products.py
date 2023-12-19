import pandas as pd
from sqlalchemy import create_engine
from credentials import db_config, db_config_silver

# Establish a connection to the Bronze PostgreSQL database
bronze_engine = create_engine(
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
    f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# Read data from the products table of the Bronze layer
df_products = pd.read_sql_table('products', bronze_engine, schema='csv')

# Rename product_id to id_product, cast varchar fields, and reorganise the columns
df_products.rename(columns={'product_id': 'id_product'}, inplace=True)
df_products = df_products[['id_product', 'name', 'price', 'inventory', 'load_date']]
df_products['id_product'] = df_products['id_product'].astype(str).str[:75]
df_products['name'] = df_products['name'].astype(str).str[:75]

# Show the first rows to verify the changes
print(df_products.head())

# Establish a connection to the Silver PostgreSQL database
silver_engine = create_engine(
    f"postgresql+psycopg2://{db_config_silver['user']}:{db_config_silver['password']}@"
    f"{db_config_silver['host']}:{db_config_silver['port']}/{db_config_silver['database']}"
)

# Load the transformed data into a new table in the Silver zone
df_products.to_sql('trf_products', silver_engine, if_exists='replace', index=False, schema='csv')
