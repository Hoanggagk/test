import requests
import psycopg2
import time
import datetime
import csv
import sys
import argparse
import logging
import os
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "scada_collection.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("scada_collector")

# ====== CONFIG ======
GOOGLE_SHEET_ID = os.environ.get('GOOGLE_SHEET_ID')
SHEET_NAME = 'SCADA_Data'
# Database connection string
DB_CONNECTION_STRING = 'postgresql://windforecasting_user:j27tj5s12XJEzj6COLNn8xXRA28BW39L@dpg-d083mms9c44c73bg79u0-a/windforecasting'
# Individual database connection parameters (extracted from connection string for compatibility)
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', '5432')  # Default PostgreSQL port
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
FARM_ID = int(os.environ.get('FARM_ID', 1))  # ID of the wind power farm this data belongs to
RETRY_INTERVAL = 30  # Seconds to wait before retrying if connection fails
MAX_RETRIES = 5  # Maximum number of retries for database connection
TIMEZONE = datetime.timezone(datetime.timedelta(hours=7))  # UTC+7 for Vietnam

# Log startup configuration (without sensitive info)
logger.info(f"Starting SCADA collector with configuration:")
logger.info(f"- DB_HOST: {DB_HOST}")
logger.info(f"- DB_NAME: {DB_NAME}")
logger.info(f"- FARM_ID: {FARM_ID}")
logger.info(f"- Google Sheet ID: {GOOGLE_SHEET_ID}")
logger.info(f"- Sheet Name: {SHEET_NAME}")

# ====== FETCH DATA FROM GOOGLE SHEET ======
def get_google_sheets_data():
    """
    Fetches data from Google Sheets
    
    Returns:
        List of rows from the Google Sheet
    
    Raises:
        Exception: If there is an error fetching the data
    """
    try:
        url = f'https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
        logger.info(f"Fetching data from Google Sheet")
        response = requests.get(url, timeout=30)  # Increased timeout for container environments
        response.raise_for_status()
        csv_data = response.text

        # Use CSV library to read data
        reader = csv.reader(csv_data.splitlines())
        data = list(reader)
        logger.info(f"Successfully retrieved {len(data)} rows of data")
        return data
    except requests.RequestException as e:
        logger.error(f"Error fetching data from Google Sheets: {e}")
        raise
    
def process_data(data):
    """
    Extracts measurement data from Google Sheet rows.
    
    Args:
        data: List of rows from the Google Sheet
        
    Returns:
        Dictionary containing the extracted measurement data
        
    Raises:
        IndexError: If the data structure doesn't match expected format
        ValueError: If there's an issue converting values to float
    """
    try:
        def convert_to_float(value):
            if value and value.strip():
                return float(value.replace(',', '.'))
            return None
            
        return {
            "farm_id": FARM_ID,
            "power_output": convert_to_float(data[21][1]),
            "available_capacity": None,
            "wind_speed": convert_to_float(data[23][1]),
            "wind_direction": convert_to_float(data[24][1]),
            "temperature": convert_to_float(data[25][1])
        }
    except (IndexError, ValueError) as e:
        logger.error(f"Error parsing data: {e}")
        logger.debug(f"Data structure: {data}")
        raise

# ====== DATABASE OPERATIONS ======
def connect_to_database():
    """
    Establishes a connection to the PostgreSQL database with retry capability
    
    Returns:
        Connection object to the database
        
    Raises:
        Exception: If unable to connect after max retries
    """
    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            logger.info(f"Attempting to connect to database (attempt {retry_count + 1}/{MAX_RETRIES})")
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            logger.info("Successfully connected to database")
            return conn
        except psycopg2.OperationalError as e:
            retry_count += 1
            if retry_count >= MAX_RETRIES:
                logger.error(f"Failed to connect to database after {MAX_RETRIES} attempts: {e}")
                raise
            logger.warning(f"Database connection failed: {e}. Retrying in {RETRY_INTERVAL} seconds...")
            time.sleep(RETRY_INTERVAL)

def save_to_database(data):
    """
    Saves the processed data to the PostgreSQL database
    
    Args:
        data: Dictionary containing measurement data
        
    Returns:
        Boolean indicating success/failure
    """
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Current timestamp in Vietnam timezone (UTC+7)
        timestamp = datetime.datetime.now(TIMEZONE)
        
        # Insert data into measurements table
        query = """
        INSERT INTO scada_data
        (farm_id, timestamp, power_output, available_capacity, wind_speed, wind_direction, temperature)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(
            query, 
            (

                data["farm_id"],
                timestamp,
                data["power_output"],
                data["available_capacity"],
                data["wind_speed"],
                data["wind_direction"],
                data["temperature"]
            )
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully saved data for farm {data['farm_id']} at {timestamp}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving data to database: {e}")
        if 'conn' in locals() and conn:
            conn.close()
        return False

def wait_until_next_minute():
    """
    Waits until the beginning of the next minute using Vietnam timezone (UTC+7).
    
    Returns:
        Datetime object of the next minute in Vietnam time
    """
    now = datetime.datetime.now(TIMEZONE)
    # Calculate time until the next minute
    next_minute = now.replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)
    wait_seconds = (next_minute - now).total_seconds()
    
    logger.info(f"Waiting for {wait_seconds:.2f} seconds until next minute ({next_minute.strftime('%H:%M:%S')} Vietnam time)")
    time.sleep(wait_seconds)
    return next_minute

def main():
    """
    Main function that orchestrates the data collection and storage process
    """
    try:
        # Fetch data from Google Sheets
        sheet_data = get_google_sheets_data()
        
        # Process the data
        measurement_data = process_data(sheet_data)
        
        # Save to database
        if save_to_database(measurement_data):
            logger.info("Data collection cycle completed successfully")
        else:
            logger.error("Failed to complete data collection cycle")
            
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Collect SCADA data from Google Sheets and store in database')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    parser.add_argument('--interval', type=int, default=1, help='Collection interval in minutes')
    
    args = parser.parse_args()
    
    if args.once:
        sys.exit(main())
    else:
        logger.info(f"Starting continuous collection with {args.interval} minute interval (Vietnam time)")
        while True:
            try:
                # Wait until the next minute
                next_run_time = wait_until_next_minute()
                logger.info(f"Starting collection at {next_run_time.strftime('%Y-%m-%d %H:%M:%S')} Vietnam time")
                
                # Run the data collection
                main()
                
                # If interval is > 1 minute, calculate remaining wait time
                if args.interval > 1:
                    # Calculate the next run time
                    now = datetime.datetime.now(TIMEZONE)
                    minutes_to_wait = args.interval - 1  # We already waited for the current minute
                    
                    # Only sleep if there's time left to wait
                    if minutes_to_wait > 0:
                        logger.info(f"Waiting {minutes_to_wait} more minutes until next collection...")
                        time.sleep(minutes_to_wait * 60)
                
            except KeyboardInterrupt:
                logger.info("Collection stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in collection loop: {e}")
                # Even on error, try to maintain the minute-aligned schedule
                time.sleep(30)  # Wait 30 seconds before trying again

