import pandas as pd
import numpy as np
import os

# Parameters
num_records = 1000  # Number of sales records to generate
products = ['Product A', 'Product B', 'Product C', 'Product D']
dates = pd.date_range('2023-01-01', periods=90).tolist()
regions = ['North', 'South', 'East', 'West']

# Generate synthetic data
np.random.seed(42)  # For reproducibility
data = {
    'Date': np.random.choice(dates, num_records),
    'Product': np.random.choice(products, num_records),
    'Quantity': np.random.randint(1, 20, num_records),
    'Price': np.round(np.random.uniform(10.0, 100.0, num_records), 2),
    'Region': np.random.choice(regions, num_records),
}

sales_data = pd.DataFrame(data)
sales_data['Date'] = pd.to_datetime(sales_data['Date'])

# Ensure the data directory exists
output_dir = '../data'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Save to CSV
sales_data.to_csv(os.path.join(output_dir, 'sales_data.csv'), index=False)

print("Synthetic data generated and saved to 'data/sales_data.csv'")
