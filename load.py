import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import os

# --- CONFIGURATION ---
# We can re-use all the same project details from before
PROJECT_ID = "331982935310"
KEY_PATH = "service_account_key.json"

# --- NEW TABLE DETAILS ---
DATASET_ID = "spotify_data"
TABLE_ID = "tracks"

# --- FILE TO LOAD ---
CLEAN_FILE_NAME = "spotify_cleaned.csv"

# ---------------------

def load_to_bigquery():
    """Reads the clean CSV and uploads it to a new BigQuery table."""

    # 1. READ THE CLEAN CSV
    print(f"Reading clean data from {CLEAN_FILE_NAME}...")
    try:
        df = pd.read_csv(CLEAN_FILE_NAME)
        print("Data loaded into Pandas successfully.")
        print(f"Shape to load: {df.shape}")

        # BigQuery is picky about column names.
        # Let's replace any special characters just in case.
        df.columns = df.columns.str.replace('[^A-Za-z0-9_]+', '', regex=True)
        print("Column names cleaned.")

    except FileNotFoundError:
        print(f"ERROR: Could not find {CLEAN_FILE_NAME}. Please run transform.py first.")
        return
    except Exception as e:
        print(f"ERROR reading CSV: {e}")
        return

    # 2. CHECK CREDENTIALS
    if not os.path.exists(KEY_PATH):
        print(f"\nWARNING: Service account key file '{KEY_PATH}' not found.")
        print("Skipping BigQuery upload step.")
        print("To enable BigQuery upload, place your JSON key file in the project directory")
        print(f"and name it '{KEY_PATH}', or update the KEY_PATH variable in this script.")
        return

    # 3. LOAD TO BIGQUERY
    print(f"Loading data into BigQuery table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}...")

    try:
        credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
        pandas_gbq.to_gbq(
            df,
            destination_table=f"{DATASET_ID}.{TABLE_ID}",
            project_id=PROJECT_ID,
            credentials=credentials,
            if_exists='replace'  # 'replace' will create a new table or overwrite
        )
        print("\nSUCCESS! Data loaded to BigQuery.")

    except Exception as e:
        print(f"\nERROR loading data to BigQuery: {e}")

# This makes the script runnable from the command line
if __name__ == "__main__":
    load_to_bigquery()
