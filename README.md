# Plant_Shop

## Project Objective
The aim of this project is to replicate my final data engineering course project done with Cívica Software, but using different technologies to compare the differences between the Modern Data Stack technologies they taught us and the older ones I've used in this project.
Final data engineering course project using technologies like Fivetran, Snowflake, and dbt: https://github.com/enriqueHG94/curso_data_engineering

## Project Structure
The project is structured in the Lakehouse medallion architecture:
- **Bronze (Raw Data Zone)**: Stores unprocessed raw data.
- **Silver (Cleansed Data Zone)**: Contains clean and transformed data ready for modeling.
- **Gold (Dimensional Model Zone)**: Includes dimensional modeling with star schemas, optimized for analysis.

## Implemented Use Cases
1. User purchase analysis.
2. User session summary.
3. Monthly sales analysis by product.

## Technologies Used
- Python 3.10.0
- PostgreSQL 16.1
- Python Libraries: pandas 2.1.4, sqlalchemy 2.0.23, psycopg2-binary-2.9.9

## Contact
Email: enriquehervasguerrero@gmail.com 
LinkedIn: https://www.linkedin.com/in/enrique-hervas-guerrero/
