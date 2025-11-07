import pandas as pd

# Define the filename
FILE_NAME = "genres_v2.csv"

print(f"Loading data from {FILE_NAME}...")

try:
    # 1. LOAD THE DATA
    df = pd.read_csv(FILE_NAME)

    # 2. EXPLORE: Print the first 5 rows
    print("\n--- First 5 Rows (Head) ---")
    # We use .head() to peek at the data
    print(df.head())

    # 3. EXPLORE: Print column info
    print("\n--- Column Info (Data Types & Nulls) ---")
    # We use .info() to see data types and missing values
    df.info()

except FileNotFoundError:
    print(f"Error: Could not find the file {FILE_NAME}")
except Exception as e:
    print(f"An error occurred: {e}")