import hashlib
import pandas as pd
from pytz import utc
from sqlalchemy import create_engine
from credentials import db_config, db_config_silver

# Establish a connection to the Bronze PostgreSQL database
bronze_engine = create_engine(
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
    f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# Read data from the budget table of the Bronze layer
df_orders = pd.read_sql_table('orders', bronze_engine, schema='csv')

# Rename columns
df_orders.rename(columns={
    'order_id': 'id_order',
    'user_id': 'id_user',
    'address_id': 'id_address',
    'tracking_id': 'id_tracking',
    'promo_id': 'id_promo',
    'created_at': 'order_created_utc',
    'estimated_delivery_at': 'estimated_delivery_at_utc',
    'delivered_at': 'delivery_utc',
    'shipping_cost': 'shipping_cost_usd',
    'order_cost': 'order_cost_usd',
    'order_total': 'order_total_usd'
}, inplace=True)

# Replace null values with 'Without Promotion'
df_orders['id_promo'].fillna('Without Promotion', inplace=True)

# Replace null values with 'Awaiting Tracking'
df_orders['id_tracking'].fillna('Awaiting Tracking', inplace=True)

# Replace null values with 'Awaiting service'
df_orders['shipping_service'].fillna('Awaiting Service', inplace=True)

# Change time zone to UTC and format
df_orders['order_created_utc'] = pd.to_datetime(df_orders['order_created_utc']).dt.tz_convert(utc)
df_orders['delivery_utc'] = pd.to_datetime(df_orders['delivery_utc']).dt.tz_convert(utc)

# Cast varchar fields to a maximum of 75 characters
varchar_columns = ['id_order', 'id_user', 'id_address', 'id_tracking', 'id_promo', 'shipping_service', 'status']
for col in varchar_columns:
    df_orders[col] = df_orders[col].astype(str).str[:75]

# Generating a surrogate key (SK) for id_promo using a hash function
df_orders['id_promo'] = [hashlib.sha256(str.encode(id_promo)).hexdigest() for id_promo in df_orders['id_promo']]

# Reorder columns
df_orders = df_orders[
    ['id_order', 'id_user', 'id_address', 'id_tracking',
     'id_promo', 'status', 'shipping_service', 'order_created_utc',
     'estimated_delivery_at_utc', 'delivery_utc', 'shipping_cost_usd',
     'order_cost_usd', 'order_total_usd', 'load_date']]

# Show the first rows to verify the changes
pd.set_option('display.max_columns', None)  # Displays all columns
pd.set_option('display.expand_frame_repr', False)  # Avoid multiple line representation
print(df_orders.head())

# Establish a connection to the Silver PostgreSQL database
silver_engine = create_engine(
    f"postgresql+psycopg2://{db_config_silver['user']}:{db_config_silver['password']}@"
    f"{db_config_silver['host']}:{db_config_silver['port']}/{db_config_silver['database']}"
)

# Load the transformed data into a new table in the Silver zone
df_orders.to_sql('trf_orders', silver_engine, if_exists='replace', index=False, schema='csv')
