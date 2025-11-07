import pandas as pd
import numpy as np

FILE_NAME = "genres_v2.csv"
CLEAN_FILE_NAME = "spotify_cleaned.csv"

# 1. LOAD THE DATA
print(f"Loading {FILE_NAME}...")
# We add low_memory=False to silence the DtypeWarning
df = pd.read_csv(FILE_NAME, low_memory=False)

print("Data loaded. Starting transformation...")
print(f"Original shape: {df.shape}")

# 2. TRANSFORM: Drop unnecessary columns
columns_to_drop = [
    'Unnamed: 0',
    'type',
    'id',
    'uri',
    'track_href',
    'analysis_url'
]
df = df.drop(columns=columns_to_drop)

# 3. TRANSFORM: Merge 'song_name' and 'title'
# We'll create a new column 'song_title'
# It takes the value from 'song_name' first.
# If 'song_name' is empty (null), it then takes the value from 'title'.
df['song_title'] = df['song_name'].fillna(df['title'])

# 4. TRANSFORM: Drop the old 'song_name' and 'title' columns
df = df.drop(columns=['song_name', 'title'])

# 5. TRANSFORM: Drop any rows that STILL don't have a title
# This cleans up any remaining bad data
df.dropna(subset=['song_title'], inplace=True)

# 6. TRANSFORM: Drop any perfect duplicates
df.drop_duplicates(inplace=True)

# 7. DONE! Print the info of our new, clean DataFrame
print("\n--- Cleaned Data Info ---")
print(f"New shape: {df.shape}")
df.info()

# 8. SAVE the clean data
df.to_csv(CLEAN_FILE_NAME, index=False)
print(f"\nSuccessfully cleaned data and saved to {CLEAN_FILE_NAME}")