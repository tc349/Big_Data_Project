#!/usr/bin/env python3

import os
import sys
import argparse
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from urllib.parse import quote_plus

def load_environment(env_path: str = None):
    load_dotenv(dotenv_path=env_path)


def create_db_engine():
    user = os.getenv("DB_USERNAME")
    password = quote_plus(os.getenv("DB_PASSWORD", ""))
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database = os.getenv("DB_NAME")

    if not all([user, password, host, port, database]):
        print("One or more database environment variables are missing.")
        sys.exit(1)

    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    try:
        engine = create_engine(conn_str, pool_pre_ping=True)
        print("Database engine created successfully.")
        return engine
    except SQLAlchemyError as e:
        print("Failed to create database engine.")
        sys.exit(1)


def full_load(engine, csv_path, table_name):
    try:
        df = pd.read_csv(csv_path)
        print(f"Read {len(df)} rows from {csv_path}.")
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"Full load complete: table '{table_name}' replaced.")
    except Exception as e:
        print("Full load failed.")
        print(f"Error: {e}")
        raise


def incremental_load(engine, csv_path, table_name):

    try:
        df = pd.read_csv(csv_path)
        print(f"Read {len(df)} rows from {csv_path}.")
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"Incremental load complete: data appended to '{table_name}'.")
    except Exception as e:
        print("Incremental load failed.")
        raise

def stream_tbl_postgres(engine, csv_path, table_name):

    try:
        df = pd.read_csv(csv_path)
        print(f"Read {len(df)} rows from {csv_path}.")
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"Streaming table created: data appended to '{table_name}'.")
    except Exception as e:
        print("Streaming table not created.")
        raise

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["full", "inc","stream"],)
    return parser.parse_args()
    

def main():
    args = parse_args()
    load_environment()
    engine = create_db_engine()

    # pick up defaults or override with env vars
    full_csv = os.getenv("FULL_LOAD_CSV", "../data/split/full_load.csv")
    inc_csv = os.getenv("INCREMENTAL_LOAD_CSV", "../data/split/inc_load.csv")
    streaming_csv = os.getenv("KAFKA_STREAMING_CSV", "../data/split/kafka_streaming.csv")
    table = os.getenv("LOAD_TABLE", "bd_class_project")
    # inc_table = os.getenv("INCREMENTAL_LOAD_TABLE", "bd_class_project")

    try:
        # full_load(engine, full_csv, full_table)
        # incremental_load(engine, inc_csv, inc_table)
        if args.mode == "full":
            full_load(engine, full_csv, table)
        if args.mode == "inc":
            incremental_load(engine, inc_csv, table)
        if args.mode == "stream":
            stream_tbl_postgres(engine, streaming_csv, "cc_fraud_streaming_data")
    except Exception as e:
        print("ETL job failed.")
        sys.exit(1)
    finally:
        engine.dispose()
        print("Database connection closed.")


if __name__ == "__main__":
    main()
