import sqlite3
import pandas as pd

INPUT_CSV = "data/processed/cleaned_RC_Book.csv"
DB_PATH = "storage/library.db"
TABLE_NAME = "books"

def load_data(csv_path):
    """Load CSV data into a pandas DataFrame for database import.
    Args:
        csv_path (str): Path to the CSV file to load.
    Returns:
        pandas.DataFrame: Loaded data frame.
    """
    print(f"Loading data from {csv_path}")
    return pd.read_csv(csv_path, encoding="latin-1", low_memory=False)

def create_connection(db_path):
    """Create and return a SQLite database connection.
    Args:
        db_path (str): Path to the SQLite database file.
    Returns:
        sqlite3.Connection: Database connection object.
    """
    conn = sqlite3.connect(db_path)
    print(f"Connected to database at {db_path}")
    return conn

def create_table(conn):
    """Create the `books` table in the provided database connection.
    Drops any existing `books` table first to ensure the table schema is
    recreated cleanly.
    Args:
        conn (sqlite3.Connection): Active database connection.
    """
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS books;")

    cursor.execute("""
    CREATE TABLE books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        isbn TEXT NOT NULL,
        title TEXT NOT NULL,
        author TEXT NOT NULL,
        description TEXT,
        source TEXT,
        year INTEGER,
        acc_date TEXT,
        place_publisher TEXT
    );
    """)

    conn.commit()

    print("Table Created Successfully.")

def insert_data(conn,df):
    """Insert rows from the DataFrame into the `books` table.
    Args:
        conn (sqlite3.Connection): Active database connection.
        df (pandas.DataFrame): Data to insert. Expects columns like
            'ISBN', 'Title', 'Author', 'book_description',
            'description_source', 'Year', 'Acc_Date', 'Place_&_Publisher'.
    """
    cursor = conn.cursor()

    # Insert Rows

    insert_sql = """
    INSERT OR IGNORE INTO books
    (isbn, title, author, description, source, year, acc_date, place_publisher)
    VALUES (?, ?, ?, ?, ?,? ,? ,?)
    """

    for _, row in df.iterrows():
        cursor.execute(
            insert_sql,
            (
                row["ISBN"],
                row["Title"],
                row["Author"],
                row["book_description"],
                row["description_source"],
                row['Year'],
                row['Acc_Date'],
                row['Place_&_Publisher']
            )
        )
    conn.commit()


    print("Data Moved SUccessfully.")

def verify_data(conn):
    """Print a simple count of records in the `books` table.
    Args:
        conn (sqlite3.Connection): Active database connection.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM books;")
    count = cursor.fetchone()[0]
    print(f"Total records in the books table: {count}")

def main_db():
    """High-level function to load CSV data into the SQLite database.
    Performs loading, table creation, data insertion and a verification
    count. Intended to be used as a pipeline step.
    """
    df = load_data(INPUT_CSV)
    conn = create_connection(DB_PATH)
    create_table(conn)
    insert_data(conn, df)
    verify_data(conn)

if __name__ == "__main__":
    main_db()
    



            