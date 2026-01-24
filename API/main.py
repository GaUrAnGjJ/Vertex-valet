from fastapi import FastAPI, HTTPException
import sqlite3

app = FastAPI(title="Books API")

DB_PATH = "../storage/library.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row 
    return conn

@app.get("/")
def health_check():
    return {"status": "API is running"}

## Find Book By ISBN

@app.get("/books/{isbn}")
def get_book_by_isbn(isbn: str):
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
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT isbn, title, author
        FROM books
        WHERE title LIKE ? OR author LIKE ?
        LIMIT 20
    """, (f"%{q}%", f"%{q}%"))

    results = cursor.fetchall()
    conn.close()

    return [dict(r) for r in results]
