# Plant_Shop

## Description
The Plant_Shop project is a data warehouse that models and analyzes the operations of a virtual plant store. It uses ELT to process data from ingestion to transformation, preparing the data for complex analysis and data-driven decision-making.

## Project Objective
The aim of this project is to replicate my final project of the data engineering course with Civica Software, but using different technologies to compare the differences between the Modern Data Stack technologies that we were taught, which are newer, against other technologies that I have used in this project and have been on the market for a longer time.
Final data engineering course project using technologies like Fivetran, Snowflake, and dbt: https://github.com/enriqueHG94/curso_data_engineering

## Project Structure
The project is structured in the Lakehouse medallion architecture:
- **Bronze (Raw Data Zone)**: Stores unprocessed raw data.
- **Silver (Cleansed Data Zone)**: Contains clean and transformed data ready for modeling.
- **Gold (Dimensional Model Zone)**: Includes dimensional modeling with star schemas, optimized for analysis.

## Implemented Use Cases
1. Analysis of user purchases.
2. Summary of user sessions.
3. Monthly sales analysis by product.

## How to Start
- Python 3.10.0
- PostgreSQL 16.1
- Python Libraries: pandas 2.1.4, sqlalchemy 2.0.23, psycopg2-binary-2.9.9

### Installation
1. Clone the repository to your local machine using `git clone https://github.com/enriqueHG94/Plant_Shop.git`.
2. Install the necessary dependencies with `pip install -r requirements.txt`.

### Database Setup
- Ensure you have a PostgreSQL instance running.
- Create corresponding databases for the bronze, silver, and gold zones, then create the csv schema in each database. In the bronze database, load or copy the code in the file create_schema_tables_bronze.sql and execute it in PostgreSQL.
- Configure the credentials.py file to connect to your databases.

### Running Scripts
- Ensure you have a bronze folder with 8 csv files.
- Run the extract_and_load.py file to extract data from the CSVs and load it into your bronze database in the csv schema in the tables you previously created.
- Open the silver folder and execute each of the scripts within the folder; each one performs transformations for each of the tables. Then, go to your silver database, and you should have 8 new tables in your csv schema.
- Load or copy the code in the file alter_pk_silver.sql and execute it in PostgreSQL to indicate the primary key in your tables. (This is necessary because pandas lose the primary key when extracting data to convert it into a pandas dataframe.)
- Open the gold folder and first execute each of the .py files that start with fct_ or dim_. These files create the dimensional model of the data in your gold database.
- After loading the data into your gold database, look in the gold folder; there will be 3 folders each with a different name. Open them and execute the .py files inside. These files are the use cases I have implemented with the data you previously loaded into your gold database.
- Finally, load or copy the file alter_pk_gold.sql in PostgreSQL and execute it to have the primary keys ready in your gold database. If everything has gone well, you will have completed the ELT process and will have a database in gold ready for analysis with three example use cases.

## Contact
- Email: enriquehervasguerrero@gmail.com
- LinkedIn: [Enrique Hervas Guerrero](https://www.linkedin.com/in/enrique-hervas-guerrero/)
