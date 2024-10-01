import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from itertools import islice
from functools import partial

# MySQL connection setup
def create_db_connection(host_name, user_name, user_password, db_name):
    """Creates a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
        return connection
    except Error as e:
        print(f"The error '{e}' occurred")
        return None

# Lazy reader for the CSV using Pandas
def lazy_csv_reader(csv_file, chunk_size=1000):
    """Reads CSV in chunks lazily using a generator."""
    # The chunksize parameter allows lazy loading
    return pd.read_csv(csv_file, delimiter=';', chunksize=chunk_size)

# Transform functions
def transform_row(row):
    """Transforms a single row to match the database schema."""
    return (
        row['Situação'], 
        row['Irmão'], 
        pd.to_datetime(row['Vencimento'], format="%d/%m/%Y").date(),
        float(row['Valor']), 
        row['Descrição'], 
        row['Tipo'], 
        row['Forma de Pagamento'] if pd.notna(row['Forma de Pagamento']) else None
    )

# Execute SQL query
def execute_batch_insert(connection, data_batch):
    """Inserts a batch of rows into the database."""
    insert_query = """
    INSERT INTO payments (situacao, irmao, vencimento, valor, descricao, tipo, forma_pagamento)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor = connection.cursor()
    try:
        cursor.executemany(insert_query, data_batch)
        connection.commit()
        print(f"Inserted batch of {len(data_batch)} rows successfully")
    except Error as e:
        print(f"The error '{e}' occurred during batch insert: {e}")
        connection.rollback()

# Load data lazily and insert into MySQL
def load_data_to_mysql(csv_chunks, connection, batch_size=100):
    """Processes chunks of CSV data and inserts into MySQL using batching."""
    for chunk in csv_chunks:
        # Lazy evaluation: Transforming rows on demand
        rows = map(transform_row, chunk.to_dict('records'))
        
        # Batch insert: Use islice for batching to limit the number of rows processed at a time
        while True:
            batch = list(islice(rows, batch_size))
            if not batch:
                break
            execute_batch_insert(connection, batch)

# ETL Process function
def etl_process(csv_file, batch_size=100):
    """The ETL main function that orchestrates the CSV loading and MySQL insertion."""
    try:
        # Create a connection to the MySQL database
        connection = create_db_connection("localhost", "root", "avidaebela", "payments_db")
        
        if connection is None:
            raise Exception("Database connection failed.")
        
        # Lazy load CSV file in chunks
        csv_chunks = lazy_csv_reader(csv_file)
        
        # Process and insert data into MySQL
        load_data_to_mysql(csv_chunks, connection, batch_size)

        # Close the database connection
        connection.close()
        print("ETL process completed.")
    
    except Exception as e:
        print(f"An error occurred during the ETL process: {e}")

# Main block to execute ETL
if __name__ == "__main__":
    try:
        # Get the current directory of the script
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # Path to the CSV file in the current folder
        csv_file = os.path.join(current_dir, "levantamento_irmaos.csv")

        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        # Start ETL process with lazy evaluation and batching
        etl_process(csv_file, batch_size=100)

    except Exception as e:
        print(f"An error occurred in the main ETL execution: {e}")
