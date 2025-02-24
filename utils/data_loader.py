# Importing required libraries
import os
import sys
import logging
import pandas as pd
import psycopg2
from dotenv import load_dotenv
# Adding paths to access the custom utility functions
sys.path.append(os.path.abspath(os.path.join('utils')))  # Updated this line
from utils import run_sql_query, populate_dataframe_to_database, create_table_query
# Load environment variables
load_dotenv()
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# Get the database connection parameters from environment variables
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DATABASE")
DB_USER = os.getenv("POSTGRES_USERNAME")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
# Connect to the PostgreSQL database
try:
    connection_params = {
        'host': "localhost",
        'port': DB_PORT,
        'database': DB_NAME,
        'user': DB_USER,
        'password': DB_PASSWORD
    }
    connection = psycopg2.connect(**connection_params)
    logger.info("Successfully connected to the PostgreSQL database.")
except psycopg2.OperationalError as e:
    logger.error("Error connecting to PostgreSQL database: %s", e)
    raise Exception("Unable to connect to the database")

# Define the root directory
root_directory = 'data/'
# Initialize SQL queries for schema creation
sql_queries = ""
# Initialize a DataFrame to hold all Totals data
all_totals_data = pd.DataFrame()

def date_changer(df):
    """
    Converts parsable dates in a DataFrame's 'Date' column to datetime format and drops rows with non-parsable dates.
    Args:
        df (pandas.DataFrame): The DataFrame containing a 'Date' column.
    Returns:
        pandas.DataFrame: The DataFrame with the 'Date' column converted to datetime format (if successful).
    """
    # Setting up the logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()  # Logs to console
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    try:
        # Attempt to convert parsable dates to datetime format
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        # Drop rows with non-parsable dates (converted to NaNs)
        df = df.dropna(subset=['Date'])
        logger.info("Successfully converted dates and dropped rows with non-parsable dates.")
        return df
    except pd.errors.ParserError as err:
        logger.error(f"Error parsing date column: {err}")
        raise

# Iterate through each folder in the root directory
for folder_name in os.listdir(root_directory):
    folder_path = os.path.join(root_directory, folder_name)
    logger.info(f"Processing folder: {folder_name}")
    # Check if the item is a directory
    if os.path.isdir(folder_path):
        chart_data_path = os.path.join(folder_path, 'Chart data.csv')
        table_data_path = os.path.join(folder_path, 'Table data.csv')
        totals_data_path = os.path.join(folder_path, 'Totals.csv')
        # Process "Chart data.csv"
        if os.path.exists(chart_data_path):
            try:
                chart_data_df = pd.read_csv(chart_data_path)
                table_name = f'{folder_name.lower().replace(" ","_")}_chart_data'
                table_query = create_table_query(chart_data_df, table_name)
                sql_queries += table_query
                logger.info(f"Generated table query for {table_name}: {table_query.strip()}")
                # Create the table and populate data
                run_sql_query(connection_params, table_query)
                chart_data_df.fillna(0, inplace=True)
                populate_dataframe_to_database(connection_params, chart_data_df, table_name)
            except Exception as e:
                logger.error(f"Error processing {chart_data_path}: {e}")
        # Process "Table data.csv"
        if os.path.exists(table_data_path):
            try:
                table_data_df = pd.read_csv(table_data_path)
                table_name = f'{folder_name.lower().replace(" ","_")}_table_data'
                table_query = create_table_query(table_data_df, table_name)
                sql_queries += table_query
                logger.info(f"Generated table query for {table_name}: {table_query.strip()}")
                # Create the table and populate data
                run_sql_query(connection_params, table_query)
                table_data_df.fillna(0, inplace=True)
                populate_dataframe_to_database(connection_params, table_data_df, table_name)
            except Exception as e:
                logger.error(f"Error processing {table_data_path}: {e}")
        # Process "Totals.csv"
        if os.path.exists(totals_data_path):
            try:
                totals_df = pd.read_csv(totals_data_path)
                all_totals_data = pd.concat([all_totals_data, totals_df], ignore_index=True)
            except Exception as e:
                logger.error(f"Error processing {totals_data_path}: {e}")

# Create the CREATE TABLE query for totals_table_data
if not all_totals_data.empty:
    totals_table_query = create_table_query(all_totals_data, 'totals_table_data')
    sql_queries += totals_table_query
    logger.info(f"Generated table query for totals_table_data: {totals_table_query.strip()}")
    # Execute the query to create the table
    run_sql_query(connection_params, totals_table_query)
    # Populate the combined Totals data into totals_table_data
    populate_dataframe_to_database(connection_params, all_totals_data, 'totals_table_data')

# Specify the file path for the SQL file
sql_file_path = '../database/db_schema.sql'

# Write the SQL queries to the file
os.makedirs(os.path.dirname(sql_file_path), exist_ok=True)
with open(sql_file_path, 'w') as sql_file:
    sql_file.write(sql_queries)

logger.info(f"SQL queries saved to {sql_file_path}")
