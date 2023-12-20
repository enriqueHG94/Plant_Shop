import pandas as pd
from sqlalchemy import create_engine
from credentials import db_config_gold

'''The marketing team needs to know for each user everything about the purchases they have made. 
   To do so, they ask us to indicate:

- All available user information
- Total number of orders placed
- Total spent
- Total shipping costs
- Total discount
- Total number of products purchased
- Total number of different products you have purchased.'''

# Establish a connection to the Gold PostgreSQL database
gold_engine = create_engine(
    f"postgresql+psycopg2://{db_config_gold['user']}:{db_config_gold['password']}@"
    f"{db_config_gold['host']}:{db_config_gold['port']}/{db_config_gold['database']}"
)
# Load the necessary tables
dim_users = pd.read_sql_table('dim_users', gold_engine, schema='csv')
fct_order_details = pd.read_sql_table('fct_order_details', gold_engine, schema='csv')
dim_promos = pd.read_sql_table('dim_promos', gold_engine, schema='csv')

# Join the tables
merged_data = fct_order_details.merge(dim_users, on='id_user', how='left').merge(dim_promos, on='id_promo', how='left')

# Grouping and calculating metrics
user_purchases = merged_data.groupby('id_user').agg(
    total_orders=pd.NamedAgg(column='id_order', aggfunc='nunique'),
    total_spent=pd.NamedAgg(column='order_total_usd', aggfunc='sum'),
    total_shipping=pd.NamedAgg(column='shipping_cost_usd', aggfunc='sum'),
    total_discount=pd.NamedAgg(column='discount', aggfunc='sum'),
    total_products=pd.NamedAgg(column='quantity', aggfunc='sum'),
    total_unique_products=pd.NamedAgg(column='id_product', aggfunc='nunique')
).reset_index()

# Selecting relevant user information from dim_users to add to the user_purchases DataFrame
user_details = dim_users[['id_user', 'id_address', 'first_name', 'last_name', 'email', 'phone_number']]

# Merging the user details into the user purchases DataFrame
user_purchases = user_purchases.merge(user_details, on='id_user', how='left')

# Reordering the DataFrame so user details come first
user_purchases = user_purchases[['id_user', 'id_address', 'first_name', 'last_name', 'email', 'phone_number',
                                 'total_orders', 'total_spent', 'total_shipping', 'total_discount',
                                 'total_products', 'total_unique_products']]

# Show the first rows to verify the changes
pd.set_option('display.max_columns', None)  # Displays all columns
pd.set_option('display.expand_frame_repr', False)  # Avoids line break in the console output
print(user_purchases.head())

# Load the final DataFrame to the Gold zone
user_purchases.to_sql('user_purchases', gold_engine, if_exists='replace', index=False, schema='csv')
