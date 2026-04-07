import json
from pathlib import Path
import time
import requests
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy import text
from urllib.parse import quote_plus

API_KEY = None
READ_ACCESS_TOKEN = None

def load_api_key():
    global API_KEY, READ_ACCESS_TOKEN
    CREDENTIALS_PATH = Path(__file__).parent.parent / "resource/api.json"

    with CREDENTIALS_PATH.open() as f:
        creds = json.load(f)

    API_KEY = creds["api_key"]
    READ_ACCESS_TOKEN = creds["read_access_token"]

    return API_KEY, READ_ACCESS_TOKEN


def load_postgres_config():
    config_path = Path(__file__).parent.parent / "resource/postgres.json"
    with config_path.open() as f:
        return json.load(f)


def create_postgres_engine():
    cfg = load_postgres_config()
    user = cfg["user"]
    password = quote_plus(cfg["password"])
    host = cfg.get("host", "localhost")
    port = cfg.get("port", 5432)
    database = cfg["database"]
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")


def fetch_movies_by_filters(
    genre_id=None,
    year=None,
    company_id=None,
    start_date=None,
    end_date=None,
    page=1,
    language="en-US",
):
    """Fetch movies from TMDB discover endpoint with filter options."""
    if not READ_ACCESS_TOKEN:
        load_api_key()

    url = "https://api.themoviedb.org/3/discover/movie"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {READ_ACCESS_TOKEN}",
    }
    params = {
        "language": language,
        "page": page,
        "sort_by": "popularity.desc",
    }

    if genre_id is not None:
        params["with_genres"] = genre_id
    if year is not None:
        params["year"] = year
    if company_id is not None:
        params["with_companies"] = company_id
    if start_date is not None:
        params["primary_release_date.gte"] = start_date
    if end_date is not None:
        params["primary_release_date.lte"] = end_date

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_movie_details(movie_id, language="en-US"):
    """Fetch one movie's details, including budget and revenue."""
    if not READ_ACCESS_TOKEN:
        load_api_key()

    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {READ_ACCESS_TOKEN}",
    }
    params = {"language": language}

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_and_prepare_movies_for_db(
    genre_id=None,
    year=None,
    company_id=None,
    start_date=None,
    end_date=None,
    pages=1,
    max_movies=50,
    language="en-US",
    delay_seconds=0.2,
    max_retries=3,
):
    """
    Fetch filtered movies, enrich details, and return DB-ready rows.
    """
    rows = []
    seen_movie_ids = set()

    for page in range(1, pages + 1):
        listing = fetch_movies_by_filters(
            genre_id=genre_id,
            year=year,
            company_id=company_id,
            start_date=start_date,
            end_date=end_date,
            page=page,
            language=language,
        )
        results = listing.get("results", [])

        for movie in results:
            movie_id = movie.get("id")
            if not movie_id or movie_id in seen_movie_ids:
                continue
            if len(rows) >= max_movies:
                return rows

            seen_movie_ids.add(movie_id)

            details = None
            for attempt in range(1, max_retries + 1):
                try:
                    details = fetch_movie_details(movie_id, language=language)
                    break
                except requests.RequestException:
                    if attempt == max_retries:
                        details = None
                    else:
                        time.sleep(delay_seconds * attempt)

            if not details:
                continue

            budget = details.get("budget") or 0
            revenue = details.get("revenue") or 0
            roi = None
            if budget > 0:
                roi = (revenue - budget) / budget

            rows.append(
                {
                    "movie_id": details.get("id"),
                    "title": details.get("title"),
                    "original_title": details.get("original_title"),
                    "release_date": details.get("release_date"),
                    "original_language": details.get("original_language"),
                    "adult": details.get("adult"),
                    "popularity": details.get("popularity"),
                    "vote_average": details.get("vote_average"),
                    "vote_count": details.get("vote_count"),
                    "budget": budget,
                    "revenue": revenue,
                    "roi": roi,
                    "genre_ids": [g.get("id") for g in details.get("genres", [])],
                    "production_company_ids": [
                        c.get("id") for c in details.get("production_companies", [])
                    ],
                }
            )
            time.sleep(delay_seconds)

    return rows

def process_and_load_data(db_rows):
    """
    Load rows returned by fetch_and_prepare_movies_for_db into PostgreSQL.
    """
    if not db_rows:
        print("No rows to load.")
        return

    engine = create_postgres_engine()
    df_raw = pd.DataFrame(db_rows)
    
    # ---------------------------------------------------------
    # 1. Dim_Movies
    # ---------------------------------------------------------
    df_dim_movies = df_raw[
        ["movie_id", "title", "original_title", "release_date", "original_language", "adult"]
    ].copy()
    df_dim_movies.drop_duplicates(subset=['movie_id'], inplace=True)
    with engine.connect() as conn:
        existing_movie_ids = {
            row[0] for row in conn.execute(text("SELECT movie_id FROM dim_movies"))
        }
    before_dim_movies = len(df_dim_movies)
    df_dim_movies = df_dim_movies[~df_dim_movies["movie_id"].isin(existing_movie_ids)].copy()
    skipped_dim_movies = before_dim_movies - len(df_dim_movies)
    if skipped_dim_movies > 0:
        print(f"Skipped {skipped_dim_movies} existing dim_movies rows.")
    if not df_dim_movies.empty:
        df_dim_movies.to_sql('dim_movies', engine, if_exists='append', index=False)
        print("Loaded dim_movies successfully.")
    else:
        print("No new dim_movies rows to load.")

    # ---------------------------------------------------------
    # 2. Dim_Date (load parent keys before fact table)
    # ---------------------------------------------------------
    release_dates = pd.to_datetime(df_raw["release_date"], errors="coerce")
    valid_dates = release_dates.dropna().drop_duplicates()
    if not valid_dates.empty:
        df_dim_date = pd.DataFrame(
            {
                "date_key": valid_dates.dt.strftime("%Y%m%d").astype(int),
                "date": valid_dates.dt.date,
                "full_date": valid_dates.dt.date,
                "year": valid_dates.dt.year,
                "month": valid_dates.dt.month,
                "day": valid_dates.dt.day,
                "quarter": valid_dates.dt.quarter,
                "week": valid_dates.dt.isocalendar().week.astype(int),
                "day_of_week": valid_dates.dt.weekday + 1,
                "day_name": valid_dates.dt.day_name(),
            }
        )
        inspector = inspect(engine)
        dim_date_cols = {c["name"] for c in inspector.get_columns("dim_date")}
        insert_cols = [c for c in df_dim_date.columns if c in dim_date_cols]
        if "date_key" in insert_cols:
            df_dim_date = df_dim_date[insert_cols].drop_duplicates(subset=["date_key"])
            with engine.connect() as conn:
                existing_date_keys = {
                    row[0] for row in conn.execute(text("SELECT date_key FROM dim_date"))
                }
            before_dim_date = len(df_dim_date)
            df_dim_date = df_dim_date[~df_dim_date["date_key"].isin(existing_date_keys)].copy()
            skipped_dim_date = before_dim_date - len(df_dim_date)
            if skipped_dim_date > 0:
                print(f"Skipped {skipped_dim_date} existing dim_date rows.")
            if not df_dim_date.empty:
                df_dim_date.to_sql("dim_date", engine, if_exists="append", index=False)
                print("Loaded dim_date successfully.")
            else:
                print("No new dim_date rows to load.")

    # ---------------------------------------------------------
    # 3. Fact_Movies
    # ---------------------------------------------------------
    df_fact = df_raw[
        ["movie_id", "release_date", "budget", "revenue", "popularity", "vote_average", "vote_count", "roi"]
    ].copy()
    parsed_dates = pd.to_datetime(df_fact["release_date"], errors="coerce")
    df_fact["date_key"] = parsed_dates.dt.strftime("%Y%m%d")
    df_fact = df_fact[df_fact["date_key"].notna()].copy()
    df_fact["date_key"] = df_fact["date_key"].astype(int)
    df_fact.drop(columns=["release_date"], inplace=True)
    df_fact.drop_duplicates(subset=["movie_id"], inplace=True)
    # Guard: keep only facts whose date_key exists in dim_date to prevent FK violations.
    with engine.connect() as conn:
        existing_date_keys = {
            row[0] for row in conn.execute(text("SELECT date_key FROM dim_date"))
        }
    before_filter = len(df_fact)
    df_fact = df_fact[df_fact["date_key"].isin(existing_date_keys)].copy()
    skipped_count = before_filter - len(df_fact)
    if skipped_count > 0:
        print(f"Skipped {skipped_count} fact rows due to missing dim_date keys.")
    with engine.connect() as conn:
        existing_fact_movie_ids = {
            row[0] for row in conn.execute(text("SELECT movie_id FROM fact_movies"))
        }
    before_fact_dedupe = len(df_fact)
    df_fact = df_fact[~df_fact["movie_id"].isin(existing_fact_movie_ids)].copy()
    skipped_existing_fact = before_fact_dedupe - len(df_fact)
    if skipped_existing_fact > 0:
        print(f"Skipped {skipped_existing_fact} existing fact_movies rows.")
    if df_fact.empty:
        print("No new fact_movies rows to load after guards.")
    else:
        df_fact.to_sql("fact_movies", engine, if_exists="append", index=False)
        print("Loaded fact_movies successfully.")

    # ---------------------------------------------------------
    # 4. Bridge_Movie_Genre
    # ---------------------------------------------------------
    genre_records = []
    for row in db_rows:
        movie_id = row["movie_id"]
        for genre_id in row.get("genre_ids", []):
            genre_records.append({"movie_id": movie_id, "genre_id": genre_id})
            
    if genre_records:
        df_bridge_genre = pd.DataFrame(genre_records)
        df_bridge_genre.drop_duplicates(subset=["movie_id", "genre_id"], inplace=True)
        with engine.connect() as conn:
            existing_genre_pairs = {
                (row[0], row[1])
                for row in conn.execute(text("SELECT movie_id, genre_id FROM bridge_movie_genre"))
            }
        pair_series = list(zip(df_bridge_genre["movie_id"], df_bridge_genre["genre_id"]))
        df_bridge_genre = df_bridge_genre[
            [pair not in existing_genre_pairs for pair in pair_series]
        ].copy()
        if not df_bridge_genre.empty:
            df_bridge_genre.to_sql('bridge_movie_genre', engine, if_exists='append', index=False)
            print("Loaded bridge_movie_genre successfully.")
        else:
            print("No new bridge_movie_genre rows to load.")

    # ---------------------------------------------------------
    # 5. Bridge_Movie_Company
    # ---------------------------------------------------------
    company_records = []
    for row in db_rows:
        movie_id = row["movie_id"]
        for company_id in row.get("production_company_ids", []):
            company_records.append({"movie_id": movie_id, "company_id": company_id})

    if company_records:
        df_bridge_company = pd.DataFrame(company_records)
        df_bridge_company.drop_duplicates(subset=["movie_id", "company_id"], inplace=True)
        with engine.connect() as conn:
            existing_company_pairs = {
                (row[0], row[1])
                for row in conn.execute(text("SELECT movie_id, company_id FROM bridge_movie_company"))
            }
        pair_series = list(zip(df_bridge_company["movie_id"], df_bridge_company["company_id"]))
        df_bridge_company = df_bridge_company[
            [pair not in existing_company_pairs for pair in pair_series]
        ].copy()
        if not df_bridge_company.empty:
            df_bridge_company.to_sql("bridge_movie_company", engine, if_exists="append", index=False)
            print("Loaded bridge_movie_company successfully.")
        else:
            print("No new bridge_movie_company rows to load.")

if __name__ == "__main__":
    load_api_key()
    db_rows = fetch_and_prepare_movies_for_db(genre_id=28, year=2024, pages=1, max_movies=5)
    process_and_load_data(db_rows)