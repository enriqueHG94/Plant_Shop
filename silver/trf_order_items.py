import pandas as pd
from sqlalchemy import create_engine
from credentials import db_config, db_config_silver

# Establishes a connection to the Bronze database
bronze_engine = create_engine(
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
    f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# Reads data from the order_items table of the Bronze layer
df_order_items = pd.read_sql_table('order_items', bronze_engine, schema='csv')

# Rename the columns
df_order_items.rename(columns={
    'order_id': 'id_order',
    'product_id': 'id_product'
}, inplace=True)

# Casting varchar fields
df_order_items['id_order'] = df_order_items['id_order'].astype(str).str[:75]
df_order_items['id_product'] = df_order_items['id_product'].astype(str).str[:75]

# Show the first rows to verify the changes
pd.set_option('display.max_columns', None)  # Displays all columns
pd.set_option('display.expand_frame_repr', False)  # Avoid multiple line representation
print(df_order_items.head())

# Establishes a connection to the Silver database
silver_engine = create_engine(
    f"postgresql+psycopg2://{db_config_silver['user']}:{db_config_silver['password']}@"
    f"{db_config_silver['host']}:{db_config_silver['port']}/{db_config_silver['database']}"
)

# Loads the transformed data into a new table in Silver's zone.
df_order_items.to_sql('trf_order_items', silver_engine, if_exists='replace', index=False, schema='csv')
