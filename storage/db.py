import sqlite3
import pandas as pd

INPUT_CSV = "data/processed/cleaned_RC_Book.csv"
DB_PATH = "storage/library.db"
TABLE_NAME = "books"

def load_data(csv_path):
    print(f"Loading data from {csv_path}")
    return pd.read_csv(csv_path, encoding="latin-1", low_memory=False)

def create_connection(db_path):
    conn = sqlite3.connect(db_path)
    print(f"Connected to database at {db_path}")
    return conn

def create_table(conn):
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
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM books;")
    count = cursor.fetchone()[0]
    print(f"Total records in the books table: {count}")

def main_db():
    df = load_data(INPUT_CSV)
    conn = create_connection(DB_PATH)
    create_table(conn)
    insert_data(conn, df)
    verify_data(conn)

if __name__ == "__main__":
    main_db()
    



            