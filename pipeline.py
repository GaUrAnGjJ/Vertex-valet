import argparse
from urllib import parse
from ingestion.ingestion import ingestion
from transformation.transformation import transformation
from storage.db import main_db
from API.main import app
import uvicorn


def api():
    uvicorn.run(
        "API.main:app",
        reload=True
    )

def main():
    """CLI(Command-line Interface) entry point for the data pipeline.
    Supports flags to run the individual pipeline stages or the entire
    pipeline in sequence. Use `--all` to run ingestion, transformation,
    database loading and start the API server.
    """
    parser = argparse.ArgumentParser(description="BDE Data Pipeline")

    parser.add_argument("--ingestion", action="store_true")
    parser.add_argument("--transformation", action="store_true")
    parser.add_argument("--db", action="store_true")
    parser.add_argument("--api", action="store_true")
    parser.add_argument("--all", action="store_true")

    args = parser.parse_args()

    if args.all:
        ingestion()
        transformation()
        main_db()
        app()
        api()
        return

    if args.ingestion:
        ingestion()

    if args.transformation:
        transformation()

    if args.db:
        main_db()
    
    if args.api:
        api()


if __name__ == "__main__":
    main()