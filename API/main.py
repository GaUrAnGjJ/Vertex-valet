from fastapi import FastAPI, HTTPException
import sqlite3

app = FastAPI(title="Books API")

DB_PATH = "storage/library.db"

def get_db_connection():
    """Create and return a SQLite database connection.

    The returned connection uses `sqlite3.Row` as the row factory so rows
    can be accessed like dictionaries. The function reads the database
    path from the module-level `DB_PATH` constant.

    Returns:
        sqlite3.Connection: A connection object to the library database.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row 
    return conn

@app.get("/")
def health_check():
    """Health-check endpoint.

    Returns a small JSON payload indicating the API is up. Used for
    readiness/liveness checks during development and deployment.
    """
    return {"status": "API is running"}

## Find Book By ISBN

@app.get("/books/{isbn}")
def get_book_by_isbn(isbn: str):
    """Retrieve a book record by its ISBN.
    Args:
        isbn (str): The ISBN of the book to look up.
    Raises:
        HTTPException: 404 if the book is not found.
    Returns:
        dict: The book record as a dictionary.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT * FROM books WHERE isbn = ?",
        (isbn,)
    )
    book = cursor.fetchone()
    conn.close()

    if not book:
        raise HTTPException(status_code=404, detail="Book not found")

    return dict(book)

## Search by author or book name

@app.get("/search")
def search_books(q: str):
    """Search books by title or author.

    The query parameter `q` is matched against the `title` and `author`
    columns using a LIKE query. Returns up to 20 matches.
    Args:
        q (str): Search query string.
    Returns:
        List[dict]: A list of matching book records.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM books
        WHERE title LIKE ? OR author LIKE ?
        LIMIT 20
    """, (f"%{q}%", f"%{q}%"))

    results = cursor.fetchall()
    conn.close()

    return [dict(r) for r in results]
