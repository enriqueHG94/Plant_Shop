import pandas as pd
from sqlalchemy import create_engine
from credentials import db_config_silver, db_config_gold

# Establish a connection to the silver PostgreSQL database
silver_engine = create_engine(
    f"postgresql+psycopg2://{db_config_silver['user']}:{db_config_silver['password']}@"
    f"{db_config_silver['host']}:{db_config_silver['port']}/{db_config_silver['database']}"
)
# Read data from the order_items and orders tables of the silver zone
trf_order_items = pd.read_sql_table('trf_order_items', silver_engine, schema='csv')
trf_orders = pd.read_sql_table('trf_orders', silver_engine, schema='csv')

# Perform a merge between the two DataFrames using the 'id_order' column as the key.
fct_order_details = pd.merge(trf_order_items, trf_orders, on='id_order', how='left')

# Keep load_date of trf_order_items and discard that of trf_orders
fct_order_details.drop('load_date_y', axis=1, inplace=True)

# Rename the column load_date_x to remove the suffix
fct_order_details.rename(columns={'load_date_x': 'load_date'}, inplace=True)

print(fct_order_details.columns)
# Concatenate id_order and id_product to create a composite key
fct_order_details['id_order_details'] = fct_order_details['id_order'] + '_' + fct_order_details['id_product']

# Reorder columns
fct_order_details = fct_order_details[
    ['id_order_details', 'id_order', 'id_product', 'id_user', 'id_address',
     'id_promo', 'quantity', 'status', 'shipping_service', 'order_created_utc',
     'estimated_delivery_at_utc', 'delivery_utc', 'shipping_cost_usd',
     'order_cost_usd', 'order_total_usd', 'load_date']
]

# Show the first rows to verify the changes
pd.set_option('display.max_columns', None)  # Displays all columns
pd.set_option('display.expand_frame_repr', False)  # Avoid multiple line representation
print(fct_order_details)

# Establish a connection to the Gold PostgreSQL database
gold_engine = create_engine(
    f"postgresql+psycopg2://{db_config_gold['user']}:{db_config_gold['password']}@"
    f"{db_config_gold['host']}:{db_config_gold['port']}/{db_config_gold['database']}"
)

# Load the transformed data into a new table in the Gold zone
fct_order_details.to_sql('fct_order_details', gold_engine, if_exists='replace', index=False, schema='csv')