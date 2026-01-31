import pandas as pd
import time
import requests
import os

RAW_INPUT_CSV = "data/raw/RC_books.csv"
INTERMEDIATE_GOOGLE = "books_with_descriptions_safe.csv"
INTERMEDIATE_OPENLIB = "books_with_descriptions_full.csv"
FINAL_OUTPUT = "data/processed/cleaned_RC_Book.csv"

ISBN_COL = "ISBN"
SLEEP_TIME = 0.2
MAX_RETRIES = 3
SAVE_EVERY = 200


## Load CSV Data

def load_data(path):
    """Load raw CSV data into a pandas DataFrame.
    Args:
        path (str): Path to the raw CSV file.
    Returns:
        pandas.DataFrame: Loaded raw data.
    """
    print(f"Loading data from {path}")
    return pd.read_csv(path, encoding="latin-1", low_memory=False)

## Fetch description from Google Books

def fetch_google_books(isbn):
    """Fetch a book description from the Google Books API by ISBN.
    Performs a few retry attempts with a small delay on failure.
    Args:
        isbn (str): The ISBN to query.
    Returns:
        str|None: Description text if found, otherwise None.
    """
    if pd.isna(isbn):
        return None

    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"

    for _ in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                data = r.json()
                if "items" in data:
                    return data["items"][0]["volumeInfo"].get("description")
        except Exception:
            pass

        time.sleep(SLEEP_TIME)

    return None


## Fetch description from Open Library

def fetch_openlibrary(isbn):
    """Fetch a fallback description from Open Library by ISBN.
    Args:
        isbn (str): The ISBN to query.
    Returns:
        str|None: Notes or subtitle from Open Library if available.
    """
    if pd.isna(isbn):
        return None

    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"

    try:
        r = requests.get(url, timeout=5)
        data = r.json()
        key = f"ISBN:{isbn}"

        if key in data:
            return data[key].get("notes") or data[key].get("subtitle")
    except Exception:
        pass

    return None

def google_books_step(df, output_csv):
    """Primary pass to fetch descriptions from Google Books and save progress.
    This step populates a `book_description` column in `df` and periodically
    writes progress to `output_csv`.
    """
    print("Starting Google Books fetch (step 1)...")

    if "book_description" not in df.columns:
        df["book_description"] = None

    for idx, isbn in enumerate(df[ISBN_COL]):
        if pd.notna(df.at[idx, "book_description"]):
            continue

        df.at[idx, "book_description"] = fetch_google_books(isbn)
        time.sleep(SLEEP_TIME)

        if (idx + 1) % SAVE_EVERY == 0:
            df.to_csv(output_csv, index=False)
            print(f"Saved progress at row {idx + 1}")

    df.to_csv(output_csv, index=False)
    print("Google Books step 1 completed")


def openlibrary_step(input_csv, output_csv):
    """Fallback pass that fills missing descriptions using Open Library.
    Reads the intermediate CSV, fills missing `book_description` values and
    periodically writes progress to `output_csv`.
    """
    print("Starting Open Library fallback...")

    df = pd.read_csv(input_csv, encoding="latin-1", low_memory=False)
    count = 0

    for idx, row in df.iterrows():
        if pd.isna(row["book_description"]):
            df.at[idx, "book_description"] = fetch_openlibrary(row[ISBN_COL])
            count += 1
            time.sleep(SLEEP_TIME)

            if count % 200 == 0:
                df.to_csv(output_csv, index=False)
                print(f"Filled {count} rows from Open Library")

    df.to_csv(output_csv, index=False)
    print("Open Library fallback completed")


def google_books_final_step(input_csv, output_csv):
    """Final pass to give Google Books one more chance to fill missing data.
    Attempts to fill any remaining missing `book_description` entries and
    tags the source where applicable.
    """
    print("Starting Google Books final pass...")

    df = pd.read_csv(input_csv, encoding="latin-1", low_memory=False)

    for idx, isbn in enumerate(df[ISBN_COL]):
        if pd.notna(df.at[idx, "book_description"]):
            continue

        desc = fetch_google_books(isbn)
        if desc:
            df.at[idx, "book_description"] = desc
            df.at[idx, "description_source"] = "google_books"

        time.sleep(SLEEP_TIME)

        if (idx + 1) % SAVE_EVERY == 0:
            df.to_csv(output_csv, index=False)
            print(f"Saved at row {idx + 1}")

    df.to_csv(output_csv, index=False)
    print("Google Books final step completed")


## Main Pipeline

def ingestion():
    """Run the ingestion pipeline to enrich book metadata.
    The pipeline runs three steps: initial Google Books fetch, Open Library
    fallback for missing descriptions, and a final Google Books pass. The
    result is written to `FINAL_OUTPUT`.
    """
    start_time = time.time()

    df = load_data(RAW_INPUT_CSV)

    google_books_step(df, INTERMEDIATE_GOOGLE)
    openlibrary_step(INTERMEDIATE_GOOGLE, INTERMEDIATE_OPENLIB)
    google_books_final_step(INTERMEDIATE_OPENLIB, FINAL_OUTPUT)

    elapsed = (time.time() - start_time) / 3600
    print(f"\nIngestion completed successfully in {elapsed:.2f} hours")


if __name__ == "__main__":
    ingestion()

