#!/usr/bin/env python3
"""
Script to download and unzip files from a URL.

We will assume that the data is stored in a zip file and that the zip file is stored in the DATA_DIR directory,
and that the zip file has the following format:
    YYYYMM-citibike-tripdata.zip
where YYYYMM is the year and month of the data.
This format holds true for data starting in 2024.

To use this script:
    to get the most recent month of data (last month):
    python3 collectCitiBikeNycData.py

    to get all data starting from the beginning of 2024:
    python3 collectCitiBikeNycData.py --backfill

    to download a specific month of data:
    python3 collectCitiBikeNycData.py --url https://s3.amazonaws.com/tripdata/202403-citibike-tripdata.zip
"""

import os
import logging
import requests
import zipfile
import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Generator
from urllib.parse import urlparse

# Data Output Directory Path
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'raw_data')

# Data Source URL
DATA_SOURCE_URL = "https://s3.amazonaws.com/tripdata/"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_most_recent_date() -> str:
    """
    Get current date in YYYYMM format.
    
    Returns:
        str: last month in YYYYMM format
    """
    today = date.today()
    first_day_of_month = today.replace(day=1)
    return first_day_of_month - timedelta(days=1)


def generate_date_strings(start_date: date, end_date: date) -> Generator[str, None, None]:
    """
    Generate date strings in YYYYMM format for each month between start_date and end_date.
    
    Args:
        start_date (date): Start date (inclusive)
        end_date (date): End date (inclusive)
        
    Yields:
        str: Date string in YYYYMM format
    """
    current_date = start_date
    while current_date <= end_date:
        yield current_date.strftime("%Y%m")
        # Move to first day of next month
        if current_date.month == 12:
            current_date = date(current_date.year + 1, 1, 1)
        else:
            current_date = date(current_date.year, current_date.month + 1, 1)


def download_file(url: str) -> Optional[str]:
    """
    Download a file from a URL to the DATA_DIR directory.
    
    Args:
        url (str): The URL of the file to download
        
    Returns:
        Optional[str]: Path to the downloaded file if successful, None otherwise
    """
    try:
        # Create output directory if it doesn't exist
        Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
        
        # Get filename from URL
        filename = os.path.basename(urlparse(url).path)
        if not filename:
            raise ValueError(f"No filename provided for URL: {url}")
            
        output_path = os.path.join(DATA_DIR, filename)
        
        # Download the file
        logger.info(f"Downloading {url} to {output_path}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Save the file
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    
        logger.info(f"Successfully downloaded {filename}")
        return output_path
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading file: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during download: {e}")
        return None


def unzip_file(zip_path: str) -> bool:
    """
    Unzip a file to the DATA_DIR directory and delete the zip file if successful.
    
    Args:
        zip_path (str): Path to the zip file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create extraction directory if it doesn't exist
        Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
        
        # Extract the zip file
        logger.info(f"Extracting {zip_path} to {DATA_DIR}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(DATA_DIR)
            
        logger.info(f"Successfully extracted {zip_path}")
        
        # Delete the zip file after successful extraction
        os.remove(zip_path)
        logger.info(f"Deleted zip file: {zip_path}")
        
        return True
        
    except zipfile.BadZipFile as e:
        logger.error(f"Invalid zip file: {e}")
        return False
    except Exception as e:
        logger.error(f"Error extracting zip file: {e}")
        return False


def parse_args():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description='Download and process CitiBike NYC trip data'
    )
    
    parser.add_argument(
        '--backfill',
        action='store_true',
        help='Download historical data (all available data starting from 2024)'
    )
    
    parser.add_argument(
        '--url',
        type=str,
        help='Specific URL to download (overrides default behavior)'
    )
    
    return parser.parse_args()


def download_and_unzip(url: str):
    """
    Download and unzip data from a given URL.
    
    Args:
        url (str): The URL of the data to download
    """
    zip_path = download_file(url)
    if zip_path:
        # Unzip the file
        if unzip_file(zip_path):
            logger.info("Process completed successfully")
        else:
            logger.error("Failed to unzip file")
    else:
        logger.error("Failed to download file")


def main():
    """Main function to download and unzip CitiBike NYC trip data."""
    args = parse_args()
    
    if args.url:
        # If specific URL is provided, use it
        download_and_unzip(args.url)
    elif args.backfill:
        # Download all data from January 2024 until current month
        start_date = date(2024, 1, 1)
        end_date = get_most_recent_date()
        
        logger.info(f"Starting backfill from {start_date} to {end_date}")
        for date_str in generate_date_strings(start_date, end_date):
            url = f"{DATA_SOURCE_URL}{date_str}-citibike-tripdata{date_str <= '202404' and '.csv' or ''}.zip"
            logger.info(f"Processing data for {date_str}")
            download_and_unzip(url)
    else:
        # Default behavior - use the most recent data URL
        current_date = get_most_recent_date().strftime("%Y%m")
        url = f"{DATA_SOURCE_URL}{current_date}-citibike-tripdata.zip"
        download_and_unzip(url)


if __name__ == "__main__":
    main()
