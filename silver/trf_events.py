import pandas as pd
from sqlalchemy import create_engine
import pytz
from credentials import db_config, db_config_silver

# Establish a connection to the Bronze PostgreSQL database
bronze_engine = create_engine(
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
    f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# Read data from the events table of the Bronze layer
df_events = pd.read_sql_table('events', bronze_engine, schema='csv')

# Rename columns
df_events.rename(columns={
    'event_id': 'id_event',
    'user_id': 'id_user',
    'session_id': 'id_session',
    'product_id': 'id_product',
    'order_id': 'id_order',
    'created_at': 'event_created_utc'
}, inplace=True)

# Replace null values in 'id_product' with 'Paid & Sent'
df_events['id_product'].fillna('Paid & Sent', inplace=True)

# Replace null values in 'id_order' with 'Awaiting payment'
df_events['id_order'].fillna('Awaiting payment', inplace=True)


# Reorder columns
df_events = df_events[[
    'id_event', 'id_user', 'id_session', 'id_product', 'id_order',
    'event_type', 'page_url', 'event_created_utc', 'load_date'
]]

# Convert event_created_utc to UTC
df_events['event_created_utc'] = pd.to_datetime(df_events['event_created_utc']).dt.tz_convert(pytz.utc)

# Cast varchar fields to a maximum of 75 characters
for col in ['id_event', 'id_user', 'id_session', 'id_product', 'id_order', 'event_type', 'page_url']:
    df_events[col] = df_events[col].astype(str).str[:75]

# Show the first rows to verify the changes
pd.set_option('display.max_columns', None)  # Displays all columns
pd.set_option('display.expand_frame_repr', False)  # Avoid multiple line representation
print(df_events.head())
print(df_events.dtypes)
# Establish a connection to the Silver PostgreSQL database
silver_engine = create_engine(
    f"postgresql+psycopg2://{db_config_silver['user']}:{db_config_silver['password']}@"
    f"{db_config_silver['host']}:{db_config_silver['port']}/{db_config_silver['database']}"
)

# Load the transformed data into a new table in the Silver zone
df_events.to_sql('trf_events', silver_engine, if_exists='replace', index=False, schema='csv')
