import mysql.connector
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Database connection details (without 'database' key)
db_config = {
    'user': 'root',
    'password': 'Amey19989969',
    'host': 'localhost',
    'auth_plugin': 'mysql_native_password',
}

def test_connection():
    try:
        logging.info('Attempting to connect to the database server')
        conn = mysql.connector.connect(**db_config)
        logging.info('Database server connection established')
        cursor = conn.cursor()
        # Create database if it doesn't exist
        cursor.execute('CREATE DATABASE IF NOT EXISTS sales_db')
        logging.info('Database created or already exists')
        # Select the database
        cursor.execute('USE sales_db')
        logging.info('Database selected')
        # Verify the current database
        cursor.execute('SELECT DATABASE();')
        current_db = cursor.fetchone()
        logging.info(f'Current database: {current_db[0]}')
        # Get the server version
        cursor.execute('SELECT VERSION();')
        version = cursor.fetchone()
        logging.info(f'Database version: {version[0]}')
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        logging.error(f"Error: {err}")
        print(f"Error: {err}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    test_connection()
