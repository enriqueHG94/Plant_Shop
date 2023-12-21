import pandas as pd
from sqlalchemy import create_engine
from credentials import db_config_gold

# Connect to the gold database
gold_engine = create_engine(
    f"postgresql+psycopg2://{db_config_gold['user']}:{db_config_gold['password']}@"
    f"{db_config_gold['host']}:{db_config_gold['port']}/{db_config_gold['database']}"
)

# Load the necessary tables
fct_order_details = pd.read_sql_table('fct_order_details', gold_engine, schema='csv')
dim_products = pd.read_sql_table('dim_products', gold_engine, schema='csv')
fct_budget = pd.read_sql_table('fct_budget', gold_engine, schema='csv')

# Calculate the total units sold and total invoiced for each product
sales_metrics = fct_order_details.groupby('id_product').agg(
    total_units_sold=pd.NamedAgg(column='quantity', aggfunc='sum')
).reset_index()
sales_metrics = sales_metrics.merge(dim_products[['id_product', 'price']], on='id_product')
sales_metrics['total_invoiced'] = sales_metrics['total_units_sold'] * sales_metrics['price']

# Merge the sales metrics with the product details to get the name and price
product_sales_info = pd.merge(sales_metrics, dim_products[['id_product', 'name']], on='id_product', how='left')

# Calculate the budgeted units
budget_metrics = fct_budget.groupby('id_product').agg(
    units_budgeted=pd.NamedAgg(column='quantity', aggfunc='sum')
).reset_index()

# Calculate the total expected amount for each product
budget_metrics = pd.merge(
    budget_metrics,
    dim_products[['id_product', 'price']],
    on='id_product',
    how='left'
)
budget_metrics['total_expected'] = budget_metrics['units_budgeted'] * budget_metrics['price']

# Merge the sales and budget information
combined_sales_budget = pd.merge(
    product_sales_info,
    budget_metrics[['id_product', 'units_budgeted', 'total_expected']],
    on='id_product',
    how='left'
)

# Add the differences between budgeted and sold units and amount
combined_sales_budget['units_difference'] = (
        combined_sales_budget['total_units_sold'] - combined_sales_budget['units_budgeted'])
combined_sales_budget['total_difference'] = (
        combined_sales_budget['total_invoiced'] - combined_sales_budget['total_expected'])

# Convert 'order_created_utc' to datetime and extract the month
fct_order_details['month'] = pd.to_datetime(fct_order_details['order_created_utc']).dt.month

# Group by month and product_id to get units sold per month
monthly_sales = fct_order_details.groupby(['month', 'id_product']).agg(
    monthly_units_sold=pd.NamedAgg(column='quantity', aggfunc='sum')
).reset_index()

# Calculate the sales ranking by month
monthly_sales['rank_by_month'] = monthly_sales.groupby('month')['monthly_units_sold'] \
    .rank(method='first', ascending=False)

# Join the monthly ranking to the DataFrame  combined_sales_budget
combined_sales_budget = combined_sales_budget.merge(
    monthly_sales[['id_product', 'month', 'rank_by_month']],
    on='id_product',
    how='left'
)

# Reorder the columns
combined_sales_budget = combined_sales_budget[[
    'id_product', 'name', 'price', 'total_units_sold', 'units_budgeted',
    'units_difference', 'total_invoiced', 'total_expected',
    'total_difference', 'month', 'rank_by_month'
]]

# Sort the DataFrame by 'rank_by_month' so that the best-selling product is listed first
combined_sales_budget.sort_values(by=['month', 'rank_by_month'], ascending=[True, True], inplace=True)

# Show the first rows to verify the changes
pd.set_option('display.max_columns', None)  # Displays all columns
pd.set_option('display.expand_frame_repr', False)  # Avoids line break in the console output
print(combined_sales_budget)

# Load the final DataFrame to the Gold zone
combined_sales_budget.to_sql('sales_by_product_month', gold_engine, if_exists='replace', index=False, schema='csv')
