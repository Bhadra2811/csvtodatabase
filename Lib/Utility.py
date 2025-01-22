import os
import logging
import pandas as pd
import threading
import psycopg2  # Use mysql.connector if using MySQL
from configparser import ConfigParser

def setup_logging():
    log_filename = f"process_log_{pd.Timestamp.now().strftime('%Y%m%d')}.log"
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def get_config():
    config = ConfigParser()
    config.read("Config/config.ini")
    return config

def create_db_connection(config):
    try:
        conn = psycopg2.connect(
            host=config["DATABASE"]["host"],
            user=config["DATABASE"]["user"],
            password=config["DATABASE"]["password"],
            database=config["DATABASE"]["database"],
            port=config["DATABASE"]["port"]
        )
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

def create_table_if_not_exists(conn):
    query = """
    CREATE TABLE IF NOT EXISTS filtered_data (
        id INT PRIMARY KEY,
        name VARCHAR(255),
        email VARCHAR(255),
        city VARCHAR(255),
        status VARCHAR(50)
    )
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        conn.rollback()

def process_chunk(chunk, conn):
    try:
        # Filter the relevant columns
        filtered_data = chunk[["id", "name", "email", "city", "status"]]
        
        # Insert into the database
        with conn.cursor() as cur:
            for _, row in filtered_data.iterrows():
                cur.execute(
                    "INSERT INTO filtered_data (id, name, email, city, status) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;",
                    tuple(row)
                )
            conn.commit()
        logging.info("Chunk processed successfully")
    except Exception as e:
        logging.error(f"Error processing chunk: {e}")
        conn.rollback()
