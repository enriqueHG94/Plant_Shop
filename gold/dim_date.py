import pandas as pd
from sqlalchemy import create_engine
from credentials import db_config_gold

# Create a date range for the date dimension
start_date = '2020-01-01'
end_date = '2030-12-31'
dates = pd.date_range(start_date, end_date, freq='D')
df_time = pd.DataFrame(dates, columns=['date'])

# Extracts date components
df_time['time_id'] = range(1, len(df_time) + 1)
df_time['year'] = df_time['date'].dt.year
df_time['quarter'] = df_time['date'].dt.quarter
df_time['month'] = df_time['date'].dt.month
df_time['day_month'] = df_time['date'].dt.month_name()
df_time['week'] = df_time['date'].dt.isocalendar().week
df_time['day'] = df_time['date'].dt.day
df_time['day_of_week'] = df_time['date'].dt.dayofweek
df_time['day_name'] = df_time['date'].dt.day_name()
df_time['weekday'] = df_time['date'].dt.weekday
df_time['is_weekend'] = df_time['weekday'].isin([5, 6]).astype(int)
df_time['season'] = df_time['date'].dt.month % 12 // 3 + 1
df_time['fiscal_year'] = df_time['date'].dt.year + (df_time['date'].dt.month > 6).astype(int)
df_time['fiscal_quarter'] = ((df_time['date'].dt.month - 7) % 12 // 3 + 1)

# Show the first rows to verify the changes
pd.set_option('display.max_columns', None)  # Displays all columns
pd.set_option('display.expand_frame_repr', False)  # Avoids line break in the console output
print(df_time.head())


# Establish a connection to the gold PostgreSQL database
gold_engine = create_engine(
    f"postgresql+psycopg2://{db_config_gold['user']}:{db_config_gold['password']}@"
    f"{db_config_gold['host']}:{db_config_gold['port']}/{db_config_gold['database']}"
)

# Load the transformed data into a new table in the gold zone
df_time.to_sql('dim_date', gold_engine, if_exists='replace', index=False, schema='csv')
