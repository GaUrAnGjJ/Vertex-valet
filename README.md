# Vertex-Valet 

A Big Data Engineering project for processing, storing, and serving book data. This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline for book recommendations or catalog management, culminating in a REST API and a Semantic Search Engine.

## Features

- **Data Ingestion**: Multi-source ingestion pipeline to load raw book data.
- **Data Transformation**: Clean and process data, handle missing strings, and normalize formats.
- **Data Storage**: Efficient storage using SQLite with optimized schema.
- **API Service**: Fast, asynchronous REST API built with **FastAPI**.
- **Recommender System**: Semantic search capabilities using **Sentence Transformers** (`all-MiniLM-L6-v2`) to find books based on natural language descriptions (e.g., "sad story about a robot").
- **Hybrid Search**: "Smart Search" that combines keyword matching with semantic similarity.
- **Frontend**: A modern, responsive web interface for users to explore books.

## 📁 Project Structure

```
Vertex-valet/
├── pipeline.py              # Main pipeline orchestrator
├── requirements.txt         # Python dependencies
├── README.md                # This file
│
├── frontend/                # Web Interface
│   ├── index.html           # Main UI
│   ├── app.js               # Frontend Logic
│   └── styles.css           # Styling
│
├── recommender/             # Recommendation Engine
│   ├── build_embeddings.py  # Script to generate embeddings
│   ├── recommender.py       # Inference engine
│   ├── patch_metadata.py    # Utility to update metadata
│   ├── embeddings.pkl       # Vector artifacts (generated)
│   └── README.md            # Specific documentation
│
├── API/
│   └── main.py              # FastAPI application
│
├── ingestion/
│   └── ingestion.py         # Data ingestion module
│
├── transformation/
│   └── transformation.py    # Data transformation module
│
├── storage/
│   └── db.py                # Database operations module
│
├── data/
│   ├── raw/                 # Raw input
│   │   └── RC_books.csv
│   └── processed/           # Cleaned CSVs
│           └──clean_description.csv
│
└── logs/
    └── lllm.md             # Logging and documentation
```

## Installation

### Prerequisites

## Technologies Used
  Python 3.8+
  FastAPI
  SQLite
  Pandas
  Jupyter Notebooks

### Setup

1. **Create Virtual Environment**:
   ```bash
   python -m venv myvenv
   .\myvenv\Scripts\activate  # Windows
   # source myvenv/bin/activate  # Linux/Mac
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Initialize Database** (First time only):
   ```bash
   python pipeline.py --db
   ```

4. **Generate Embeddings** (Required for Recommender):
   ```bash
   python recommender/build_embeddings.py
   ```
   *Note: This downloads the 90MB model and may take a few minutes to process ~28k books.*

---

## 🧠 Recommender System

This module provides the intelligence behind the "Recommend" feature.

- **Model**: Uses `all-MiniLM-L6-v2` (a lightweight, high-performance transformer).
- **Process**: 
  1. Concatenates Book Title + Description.
  2. Generates dense vector embeddings.
  3. Calculates Cosine Similarity between user query and book vectors.
- **Hybrid Logic**: The engine prioritizes **Exact Author Matches** (boosting their score) while mixing in semantic results, allowing users to search by both "Vibe" and "Author Name" in a single bar.

You can test the recommender standalone:
```bash
python recommender/recommender.py
```

---

## 💻 Usage

### 1. Run the Full Backend Pipeline
To run the API (which automatically loads the recommender):
```bash
python pipeline.py --api
```
The API listens at `http://127.0.0.1:8000`.

### 2. Run the Frontend
In a separate terminal:
```bash
cd frontend
python -m http.server 3000
```
Open **[http://localhost:3000](http://localhost:3000)** in your browser.

### 3. Pipeline Commands
The `pipeline.py` script helps manage the ETL process:

- **Run Ingestion**: `python pipeline.py --ingestion`
- **Run Transformation**: `python pipeline.py --transformation`
- **Reset Database**: `python pipeline.py --db`
- **Run Everything**: `python pipeline.py --all`

---

## 📊 Data Snapshot

- **Raw Data**: ~36,361 records
- **Cleaned & Indexed**: ~28,481 records (Filtered for valid ISBNs and Descriptions)
- **Sources**: OpenLibrary, Google Books, Bookswagon (Data resources).
- **Final dataset columns**: `Acc_Date`, `Acc_No`, `Title`, `ISBN`, `Author_Editor`, `Edition_Volume`, `Place_Publisher`, `Year`, `Pages`, `Class_No`, `description`


---

## 🔍 API Endpoints

- **GET /**: Health check (`{"status": "API is running"}`)
- **GET /recommend?query=...**: Semantic/Hybrid search.
- **GET /books/{isbn}**: Get details by ISBN.
- **GET /search?q=...**: Legacy keyword search (Title/Author).

---

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your changes.

<div align="right">

**Vertex-Valet Team**
**Het Katrodiya**
**Gaurang Jadav**

</div>
