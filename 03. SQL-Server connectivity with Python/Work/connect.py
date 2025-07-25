import os
import logging
import pyodbc
from dotenv import load_dotenv

def create_connection():

    # Build connection string using Windows Authentication
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER={localhost_name};"
        "DATABASE={database_name};"
        "Trusted_Connection=yes;"
    )

    try:
        conn = pyodbc.connect(conn_str)
        print("✅ Connected to SQL Server successfully!")

        return conn
    except pyodbc.Error as e:   
        conn.close()
    except Exception as e:
        logging.error(f"❌ Error connecting to SQL Server: {e}")

# Run the function
if __name__ == "__main__":
    create_connection()