import pandas as pd
import os
import mysql.connector
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler('../etl.log'),
        logging.StreamHandler()  # Log messages will also be printed to console
    ]
)

# Database connection details
db_config = {
    'user': 'root',
    'password': 'Amey19989969',  # Replace with your actual MySQL root password
    'host': 'localhost',
    'auth_plugin': 'mysql_native_password',
    # 'database': 'sales_db',  # Remove or comment out this line
}

def extract_data(data_dir):
    logging.info('Starting data extraction')
    # List to hold dataframes
    data_frames = []
    # Read all CSV files in the directory
    for file in os.listdir(data_dir):
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join(data_dir, file))
            data_frames.append(df)
    if data_frames:
        # Combine all dataframes into one
        sales_data = pd.concat(data_frames, ignore_index=True)
        logging.info('Data extraction completed')
        return sales_data
    else:
        logging.error('No CSV files found in data directory')
        return None

def transform_data(sales_data):
    logging.info('Starting data transformation')
    sales_data.dropna(inplace=True)
    sales_data['Date'] = pd.to_datetime(sales_data['Date'])
    sales_data['Quantity'] = sales_data['Quantity'].astype(int)
    sales_data['Price'] = sales_data['Price'].astype(float)
    sales_data['TotalSales'] = sales_data['Quantity'] * sales_data['Price']
    sales_data['Product'] = sales_data['Product'].str.upper()
    logging.info(f'Columns after transformation: {sales_data.columns.tolist()}')
    logging.info('Data transformation completed')
    return sales_data

def load_data(sales_data):
    logging.info('Starting data loading')
    try:
        logging.info('Attempting to connect to the database server')
        conn = mysql.connector.connect(**db_config)
        logging.info('Database server connection established')
        cursor = conn.cursor()
        # Create database if it doesn't exist
        logging.info('Creating/Using database')
        cursor.execute('CREATE DATABASE IF NOT EXISTS sales_db')
        logging.info('Database created or already exists')
        # Select the database
        cursor.execute('USE sales_db')
        logging.info('Database selected')

        # Create table if it doesn't exist
        logging.info('Creating table if not exists')
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS sales (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Date DATE,
            Product VARCHAR(255),
            Quantity INT,
            Price FLOAT,
            TotalSales FLOAT,
            Region VARCHAR(255)
        );
        '''
        cursor.execute(create_table_query)
        conn.commit()
        logging.info('Table ensured')

        # Prepare data for insertion
        columns = ['Date', 'Product', 'Quantity', 'Price', 'TotalSales', 'Region']
        data_to_insert = sales_data[columns].values.tolist()
        logging.info(f'Inserting {len(data_to_insert)} records into the database')

        # Insert data into the database
        insert_query = '''
        INSERT INTO sales (Date, Product, Quantity, Price, TotalSales, Region)
        VALUES (%s, %s, %s, %s, %s, %s)
        '''
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        logging.info('Data insertion completed')
        cursor.close()
        conn.close()
        logging.info('Data loading completed')
    except mysql.connector.Error as err:
        logging.error(f"An error occurred in load_data: {err}")
        print(f"An error occurred in load_data: {err}")
    except Exception as e:
        logging.error(f"An unexpected error occurred in load_data: {e}")
        print(f"An unexpected error occurred in load_data: {e}")

def generate_report():
    logging.info('Starting report generation')
    try:
        conn = mysql.connector.connect(**db_config)
        logging.info('Database connection established')
        cursor = conn.cursor()
        # Select the database
        cursor.execute('USE sales_db')
        logging.info('Database selected')
        total_sales_query = '''
        SELECT Product, SUM(TotalSales) as TotalSales
        FROM sales
        GROUP BY Product
        ORDER BY TotalSales DESC;
        '''
        logging.info('Executing total_sales_query')
        cursor.execute(total_sales_query)
        product_sales = cursor.fetchall()
        logging.info(f'Query returned {len(product_sales)} rows')
        cursor.close()
        conn.close()

        if not product_sales:
            logging.warning('No data returned from the query')
            return

        product_sales_df = pd.DataFrame(product_sales, columns=['Product', 'TotalSales'])
        # Save report as CSV
        logging.info('Saving product_sales_report.csv')
        product_sales_df.to_csv('../product_sales_report.csv', index=False)
        logging.info('CSV report saved')

        # Generate PDF report
        logging.info('Generating PDF report')
        pdf = matplotlib.backends.backend_pdf.PdfPages("../product_sales_report.pdf")
        plt.figure(figsize=(10, 6))
        plt.bar(product_sales_df['Product'], product_sales_df['TotalSales'])
        plt.title('Total Sales per Product')
        plt.xlabel('Product')
        plt.ylabel('Total Sales')
        plt.tight_layout()
        pdf.savefig()
        pdf.close()
        logging.info('PDF report saved')
        logging.info('Report generation completed')
    except Exception as e:
        logging.error(f"An error occurred in generate_report: {e}")
        print(f"An error occurred in generate_report: {e}")

def main():
    logging.info('ETL process started')
    data_dir = '../data'
    sales_data = extract_data(data_dir)
    if sales_data is not None:
        sales_data = transform_data(sales_data)
        load_data(sales_data)
        generate_report()
        logging.info('ETL process completed successfully')
    else:
        logging.error('ETL process failed due to data extraction error')

if __name__ == '__main__':
    main()
