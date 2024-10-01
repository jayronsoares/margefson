import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from itertools import islice
from functools import partial
import streamlit as st

# MySQL connection setup with status update
def create_db_connection(host_name, user_name, user_password, db_name):
    """Creates a connection to the MySQL database with status update on Streamlit."""
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        st.success("Connection to MySQL DB successful")
        return connection
    except Error as e:
        st.error(f"Database connection error: '{e}'")
        return None

# Lazy reader for the CSV using Pandas
def lazy_csv_reader(csv_file, chunk_size=1000):
    """Reads CSV in chunks lazily using a generator."""
    return pd.read_csv(csv_file, delimiter=';', chunksize=chunk_size)

# Transform function for the data rows
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

# Execute SQL query with progress update
def execute_batch_insert(connection, data_batch, batch_number):
    """Inserts a batch of rows into the database with a progress update."""
    insert_query = """
    INSERT INTO payments (situacao, irmao, vencimento, valor, descricao, tipo, forma_pagamento)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor = connection.cursor()
    try:
        cursor.executemany(insert_query, data_batch)
        connection.commit()
        st.write(f"Inserted batch {batch_number}: {len(data_batch)} rows successfully")
    except Error as e:
        st.error(f"Error during batch {batch_number} insert: {e}")
        connection.rollback()

# Load data lazily and insert into MySQL with progress bar
def load_data_to_mysql(csv_chunks, connection, batch_size=100):
    """Processes chunks of CSV data and inserts into MySQL with a progress bar."""
    total_rows = sum([len(chunk) for chunk in csv_chunks])
    progress_bar = st.progress(0)
    total_inserted = 0

    for batch_number, chunk in enumerate(csv_chunks):
        rows = map(transform_row, chunk.to_dict('records'))

        # Batch insert
        while True:
            batch = list(islice(rows, batch_size))
            if not batch:
                break
            execute_batch_insert(connection, batch, batch_number + 1)
            total_inserted += len(batch)

            # Update progress bar
            progress = min(total_inserted / total_rows, 1.0)
            progress_bar.progress(progress)

# ETL Process function with status updates
def etl_process(csv_file, host, user, password, database, batch_size=100):
    """The ETL main function orchestrated with Streamlit for user feedback."""
    try:
        # Create a connection to the MySQL database
        connection = create_db_connection(host, user, password, database)
        if connection is None:
            raise Exception("Database connection failed.")
        
        # Lazy load CSV file in chunks
        csv_chunks = lazy_csv_reader(csv_file)

        # Process and insert data into MySQL with progress update
        load_data_to_mysql(csv_chunks, connection, batch_size)

        # Close the database connection
        connection.close()
        st.success("ETL process completed.")

    except Exception as e:
        st.error(f"An error occurred during the ETL process: {e}")

# Streamlit interface for ETL monitoring
def main():
    st.title("Margefson ETL Process Dashboard")

    # Input form for database credentials and CSV file upload
    with st.form("etl_form"):
        st.header("Database Credentials")
        host = st.text_input("MySQL Host", value="localhost")
        user = st.text_input("MySQL User", value="root")
        password = st.text_input("MySQL Password", type="password")
        database = st.text_input("MySQL Database", value="payments_db")

        st.header("Enviar Arquivo CSV")
        uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

        # Optional: Batch size input
        batch_size = st.number_input("Batch Size", min_value=1, max_value=1000, value=100)

        # Submit button
        submit_button = st.form_submit_button(label="Run ETL Process")

    # If user clicks submit, trigger the ETL process
    if submit_button:
        if uploaded_file is None:
            st.error("Por favor enviar o arquivo CSV.")
        else:
            # Save the uploaded CSV file locally
            csv_file_path = os.path.join(os.getcwd(), uploaded_file.name)
            with open(csv_file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())

            # Run the ETL process with Streamlit monitoring
            etl_process(csv_file_path, host, user, password, database, batch_size)

# Run the Streamlit app
if __name__ == "__main__":
    main()