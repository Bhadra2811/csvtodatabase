
import os
import logging
import pandas as pd
from Lib.Utility import setup_logging, get_config, create_db_connection, create_table_if_not_exists, process_chunk
from threading import Thread

if __name__ == "__main__":
    setup_logging()

    # Load configuration
    config = get_config()

    conn = None  # Initialize conn here
    try:
        # Establish database connection
        conn = create_db_connection(config)
        create_table_if_not_exists(conn)
        
        # Read CSV in chunks
        file_path = config["CSV"]["file_path"]
        chunk_size = 1000
        threads = []

        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            thread = Thread(target=process_chunk, args=(chunk, conn))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        logging.info("All chunks processed successfully.")
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
    finally:
        if conn:
            conn.close()