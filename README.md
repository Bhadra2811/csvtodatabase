# CSV to Database Processor

## Setup

1. Install dependencies:
    ```
    pip install pandas psycopg2 configparser
    ```

2. Configure the application:
    Update `Config/config.ini` with your database credentials and CSV file path.

3. Set up the database:
    Ensure your PostgreSQL or MySQL database is running and accessible.

4. Run the script:
    ```
    python Main.py
    ```

## Logs
Logs will be stored in `process_log_<YYYYMMDD>.log`.

## Notes
- Modify `chunk_size` in `Main.py` if needed for performance.
"""
#   c s v t o d a t a b a s e  
 