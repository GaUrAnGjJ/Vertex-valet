 ## Vertex-Valet 

A Big Data Engineering project for processing, storing, and serving book data. This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline for book recommendations or catalog management, culminating in a REST API built with FastAPI.

## Features

- **Data Ingestion**: Load raw book data from CSV files.
- **Data Transformation**: Clean and process the data for consistency and quality.
- **Data Storage**: Store processed data in a SQLite database.
- **API Service**: Provide RESTful endpoints to query book information by ISBN or search by title/author.

## 📁 Project Structure

```
Vertex-valet/
├── pipeline.py              # Main pipeline orchestrator
├── requirements.txt         # Python dependencies
├── README.md               # This file
│
├── API/
│   ├── __pycache__/
│   └── main.py             # FastAPI application
│
├── ingestion/
│   ├── __pycache__/
│   └── ingestion.py        # Data ingestion module
│
├── transformation/
│   ├── __pycache__/
│   └── transformation.py   # Data transformation module
│
├── storage/
│   ├── __pycache__/
│   └── db.py               # Database operations module
│
├── data/
│   ├── raw/
│   │   └── RC_books.csv    # Raw input data
│   └── processed/
│       ├── cleaned_RC_Book.csv    # After ingestion
│       └── cleaned_RC_Book1.csv   # After transformation
│
└── logs/
    └── lllm.md             # Logging and documentation
```

## Installation

### Prerequisites

- Python 3.8 or higher
- Jupyter Notebook (for running the notebooks)
- SQLite (comes with Python)


### Database Setup

1. Run the storage notebook to set up the database:
   - Open `storage/db.py` File.
   - Execute the all cells to create the SQLite database (`library.db`) and populate it with data.

## Workflow

The project follows a sequential data processing workflow:

### 1. Data Ingestion
- **File**: `ingestion/ingestion.py`
- **Purpose**: Load raw book data from CSV files.
- **Input**: `data/raw/RC_books.csv`
- **Process**:
  - Read the raw CSV file using pandas.
  - Handle encoding issues (e.g., latin-1).
  - Perform initial data exploration and cleaning if needed.
- **Output**: Raw data loaded into memory for further processing.

### 2. Data Transformation
- **File**: `transformation/transformation.py`
- **Purpose**: Clean, process, and enrich the ingested data.
- **Input**: Processed data from ingestion or `data/processed/cleaned_RC_Book.csv`
- **Process**:
  - Load the cleaned data.
  - Perform data transformations (e.g., adding descriptions, normalizing fields).
  - Handle missing values, duplicates, and data quality issues.
  - Apply business logic for data enrichment.
- **Output**: `data/processed/cleaned_RC_Book.csv` - the final processed dataset.

### 3. Data Storage
- **File**: `storage/db.py`
- **Purpose**: Store the processed data in a database for efficient querying.
- **Input**: `data/processed/cleaned_RC_Book.csv`
- **Process**:
  - Create a SQLite database schema.
  - Load the processed CSV data into the database.
  - Create indexes for performance.
  - Ensure data integrity and relationships.
- **Output**: `storage/library.db` - SQLite database with book data.

### 4. API Serving
- **File**: `API/main.py`
- **Purpose**: Provide REST API endpoints to query the book data.
- **Input**: `storage/library.db`
- **Process**:
  - Implement FastAPI application.
  - Connect to the SQLite database.
  - Define endpoints for health check, book lookup by ISBN, and search functionality.
- **Endpoints**:
  - `GET /`: Health check
  - `GET /books/{isbn}`: Get book details by ISBN
  - `GET /search?q={query}`: Search books by title or author
- **Output**: REST API responses in JSON format.


## Technologies Used
  Python 3.8+
  FastAPI
  SQLite
  Pandas
  Jupyter Notebooks

### Setup

- Install the required Python package.
- Create virutal Environment and download required packages:

```bash
1. 
  python -m venv myvenv
2.
  .\myvenv\Scripts\activate
3.
  pip install -r requirements.txt

```


## 💻 Usage

### Run Complete Pipeline
```bash
python pipeline.py --all
```

### Run Specific Components

**Only Ingestion:**
```bash
python pipeline.py --ingestion
```

**Only Transformation:**
```bash
python pipeline.py --transformation
```

**Only Database Operations:**
```bash
python pipeline.py --db
```

**Start API Server:**
```bash
python pipeline.py --api
```

The API will be available at `http://127.0.0.1:8000`.

### API Endpoints

- **GET /**: Health check endpoint.
  - Response: `{"status": "API is running"}`

- **GET /books/{isbn}**: Retrieve a book by its ISBN.
  - Parameters: `isbn` (string)
  - Response: Book details as JSON, or 404 if not found.

- **GET /search**: Search books by title or author.
  - Query Parameters: `q` (string, search query)
  - Response: List of matching books (up to 20), each with `isbn`, `title`, `author`.

### Example API Usage

- Health check: `curl http://127.0.0.1:8000/`
- Get book by ISBN: `curl http://127.0.0.1:8000/docs#/default/get_book_by_isbn_books__isbn__get`
- Search books: `curl http://127.0.0.1:8000/docs#/default/search_books_search_get`

## Data Source
- Open Library: `curl https://openlibrary.org/`
- Google Books: `curl https://books.google.co.in/`


## Data Statistics
- Raw Data : 36358
- After Removing Duplicate Data : 32012
- Description Found : 26542


## Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your changes.

<div align="right">

**Vertex-Valet**

****Het Katrodiya****  
****Gaurang Jadav****

</div>


