import requests
from pyspark.sql import SparkSession
import time
import logging

logger = logging.getLogger(__name__)

def download_file(url, retries=3):
    """
    Downloads a file from a given URL, converts it into a PySpark DataFrame, and returns it as JSON objects.

    Args:
        url (str): The URL of the file to download.
        retries (int): Number of retry attempts before quitting (default: 3).
        timeout (int): Timeout (in seconds) for each download attempt (default: 10).

    Returns:
        list: List of JSON objects representing the data in the file.
    """
    logger.info(f"Starting download from {url}")
    attempt = 0
    while attempt < retries:
        try:
            # Send a GET request to download the file
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an exception for HTTP errors

            if response.status_code == 200:
                logger.info(f"Download completed successfully")

            response_text = response.text

            spark = SparkSession.builder \
                .appName("Download to PySpark DataFrame") \
                .getOrCreate()

            # Read the string into an RDD
            rdd = spark.sparkContext.parallelize(response_text.split("\n"))

            # Convert RDD to DataFrame (assuming comma-separated values)
            df = spark.read.csv(rdd, header=True, inferSchema=True)

            json_data = df.toJSON().collect()
            return json_data

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading the file: {e}")

        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        # Retry after a short delay if download fails
        attempt += 1
        if attempt < retries:
            logger.info(f"Retrying in 5 seconds... ({attempt}/{retries})")
            time.sleep(5)

    logger.error(f"Failed to download file after {retries} attempts.")
    return None