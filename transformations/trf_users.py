import pandas as pd
from sqlalchemy import create_engine
from credentials import db_config, db_config_silver

# Connection to the Bronze PostgreSQL database
bronze_engine = create_engine(
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
    f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# Read data from the users table of the Bronze layer
df_users = pd.read_sql_table('users', bronze_engine, schema='csv')

# Renaming columns
df_users.rename(columns={
    'user_id': 'id_user',
    'address_id': 'id_address',
    'created_at': 'profile_created_utc',
    'updated_at': 'profile_updated_utc'
}, inplace=True)

# Casting varchar fields
df_users['id_user'] = df_users['id_user'].astype(str).str[:75]
df_users['id_address'] = df_users['id_address'].astype(str).str[:75]
df_users['first_name'] = df_users['first_name'].astype(str).str[:75]
df_users['last_name'] = df_users['last_name'].astype(str).str[:75]
df_users['phone_number'] = df_users['phone_number'].astype(str).str[:75]
df_users['email'] = df_users['email'].astype(str).str[:75]

# Converting timezones to UTC
df_users['profile_created_utc'] = pd.to_datetime(df_users['profile_created_utc'])
df_users['profile_updated_utc'] = pd.to_datetime(df_users['profile_updated_utc'])

# Remove the time zone offset to have "naive" timestamps in UTC
df_users['profile_created_utc'] = df_users['profile_created_utc'].dt.tz_localize(None)
df_users['profile_updated_utc'] = df_users['profile_updated_utc'].dt.tz_localize(None)

# Reorganizing columns
column_order = ['id_user', 'id_address', 'first_name', 'last_name', 'email', 'phone_number',
                'total_orders', 'profile_created_utc', 'profile_updated_utc', 'load_date']
df_users = df_users[column_order]

# Show the first rows to verify the changes
print(df_users.head())

# Connection to the Silver PostgreSQL database
engine_silver = create_engine(
    f"postgresql+psycopg2://{db_config_silver['user']}:{db_config_silver['password']}@"
    f"{db_config_silver['host']}:{db_config_silver['port']}/{db_config_silver['database']}"
)

# Load the transformed data into a new table in the Silver zone
df_users.to_sql('trf_users', engine_silver, if_exists='replace', index=False, schema='csv')
