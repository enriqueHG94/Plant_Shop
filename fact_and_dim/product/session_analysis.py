import pandas as pd
from sqlalchemy import create_engine
from credentials import db_config_gold

'''The product team needs to know for each session:

- Everything about the user of the session
- Ranking of events and total events
- Start and end of the session
- Session duration time
- Number of page views
- Number of events related to add_to_cart, checkout and package_shipped.'''

# Establish a connection to the Gold PostgreSQL database
gold_engine = create_engine(
    f"postgresql+psycopg2://{db_config_gold['user']}:{db_config_gold['password']}@"
    f"{db_config_gold['host']}:{db_config_gold['port']}/{db_config_gold['database']}"
)

# Load the necessary tables
dim_users = pd.read_sql_table('dim_users', gold_engine, schema='csv')
fct_events = pd.read_sql_table('fct_events', gold_engine, schema='csv')

# Range of events and total events per session
fct_events['event_rank'] = fct_events.groupby('id_session')['event_created_utc'].rank(method='first')
fct_events['total_events'] = fct_events.groupby('id_session')['id_session'].transform('count')

# Start, end, duration, and event details of the session
session_start_end = fct_events.groupby(['id_session', 'id_user']).agg(
    session_start=('event_created_utc', 'min'),
    session_end=('event_created_utc', 'max'),
    event_rank=('event_rank', 'first'),  # Keep the first rank of each session
    total_events=('total_events', 'first')  # Keep the total event count of each session
).reset_index()

# Duration of the session
session_start_end['session_duration'] = (pd.to_datetime(session_start_end['session_end']) -
                                         pd.to_datetime(session_start_end['session_start'])).dt.total_seconds() / 60

# Page views per session
session_page_views = fct_events.groupby('id_session')['page_url'].nunique().reset_index(name='page_views')

# Event count by type and session
event_type_counts = fct_events.pivot_table(index='id_session',
                                           columns='event_type',
                                           values='id_event',
                                           aggfunc='count').fillna(0).reset_index()

# Join all the metrics
session_info = session_start_end.merge(session_page_views, on='id_session').merge(
    event_type_counts, on='id_session'
)

# Join user and session information
final_data = session_info.merge(dim_users, on='id_user', how='left')

# Reorder columns
final_data = final_data[[
    'id_session', 'id_user', 'first_name', 'last_name', 'email', 'phone_number', 'session_start', 'session_end',
    'session_duration', 'page_views', 'add_to_cart', 'checkout', 'package_shipped', 'event_rank', 'total_events'
]]

# Show the first rows to verify the changes
pd.set_option('display.max_columns', None)  # Displays all columns
pd.set_option('display.expand_frame_repr', False)  # Avoids line break in the console output
print(final_data.head())

# Load the final DataFrame to the Gold zone
final_data.to_sql('session_analysis', gold_engine, if_exists='replace', index=False, schema='csv')
