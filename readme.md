# Spotify ETL Data Pipeline

This project is a complete ETL (Extract, Transform, Load) pipeline that processes a 13MB (40,000+ row) dataset of Spotify track features. The script cleans and merges messy data, then loads the analysis-ready file into a Google BigQuery data warehouse.

---

## 1. Project Goal

The goal was to demonstrate skills in building a robust ETL pipeline for a medium-sized, real-world dataset. This project involved handling messy data, complex transformations, and loading the final clean dataset into a cloud data warehouse for analytics.

The pipeline performs the following steps:
1.  **Extract:** Loads a 42,000-row `genres_v2.csv` file into Pandas.
2.  **Transform:**
    * Drops 7 unnecessary or blank columns (`Unnamed: 0`, `id`, `uri`, etc.).
    * Intelligently merges two separate, incomplete columns (`song_name` and `title`) into a single, complete `song_title` column.
    * Removes any remaining rows that still have null titles.
    * Removes all duplicate entries.
    * The final clean dataset has 41,787 rows and 15 complete columns.
3.  **Load:** Connects to Google Cloud and uploads the entire transformed dataset to a new table in BigQuery (`spotify_data.tracks`).

---

## 2. How to Run

### Prerequisites
* Python 3.x
* `pip` package manager

### Installation
1. Install the required dependencies:
   ```bash
   pip install pandas numpy google-cloud-bigquery pandas-gbq google-auth
   ```

### Running the Pipeline
1. **Explore the Data (Optional):**
   Run `explore.py` to see a preview of the raw data and its structure.
   ```bash
   python explore.py
   ```

2. **Extract and Transform:**
   Run `transform.py` to clean the data. This will generate a `spotify_cleaned.csv` file.
   ```bash
   python transform.py
   ```

3. **Load to BigQuery:**
   Run `load.py` to upload the cleaned data to Google BigQuery.
   ```bash
   python load.py
   ```
   *Note: To successfully upload to BigQuery, you need a valid Google Cloud Service Account key file named `service_account_key.json` in the project directory. If this file is missing, the script will skip the upload step but still verify the CSV.*

---

## 3. Tools & Architecture

* **Python 3:** The core language for the entire pipeline.
* **Libraries:**
    * `pandas`: For data exploration, transformation, and cleaning.
    * `pandas-gbq` & `google-cloud-bigquery`: For loading data into BigQuery.
* **Data Warehouse:** **Google BigQuery**
* **Authentication:** **Google Cloud Service Account**

### Simple Architecture
`genres_v2.csv` -> `Python (transform.py)` -> `spotify_cleaned.csv` -> `Python (load.py)` -> `Google BigQuery`

---

## 4. Data Analysis & Sample Queries

The final data is stored in the BigQuery table: `331982935310.spotify_data.tracks`

You can connect this table to an analytics tool like **Google Looker Studio** or query it directly with SQL to answer interesting questions.

### Cool Questions We Can Answer

* **What makes a song popular?**
    * Create a scatter plot comparing `danceability` to `energy` to see if there's a correlation.
    * Analyze the average `loudness` or `tempo` for different `genres`.

* **What are the most common song keys?**
    * Create a bar chart grouping by the `key` column (where 0=C, 1=C#, 2=D, etc.) to see which keys are most common in music.

* **What are the characteristics of different genres?**
    * Find the average `acousticness` for "Pop" vs. "Classical" genres.
    * Look at the average song length (`duration_ms`) by `genre`.

### Sample SQL Query

Here is an example query to find the top 10 most "danceable" songs in the database:

```sql
SELECT
  song_title,
  genre,
  danceability
FROM
  `331982935310.spotify_data.tracks`
ORDER BY
  danceability DESC
LIMIT 10
```
