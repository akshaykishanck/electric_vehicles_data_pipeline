import pymongo
import json
import logging
import pandas as pd

logger = logging.getLogger(__name__)
MONGO_CLIENT = "mongodb://localhost:27017/"
DB_NAME = "vehicle_db"

# Connect to MongoDB
def connect_to_mongodb(collection_name="vehicles"):
    """
    Connect to a MongoDB collection.

    :param collection_name: Name of the MongoDB collection.
    :return: MongoDB collection object.
    """
    try:
        client = pymongo.MongoClient(MONGO_CLIENT)
        db = client[DB_NAME]
        collection = db[collection_name]
        logger.info(f"Connected to collection: {collection_name}")
        return collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB collection '{collection_name}': {e}")
        raise

def save_query_output_to_new_collection(query_output, new_collection_name):
    """
    Saves the output of a MongoDB query to a new collection in the database.

    :param query_output: List of dictionaries containing query output.
    :param new_collection_name: Name of the new collection to store the output.
    :return: None
    """
    try:
        # Check if query output is empty
        if not query_output:
            logger.error("Query output is empty. No data to save.")
            raise

        # Insert query output into the new collection
        new_collection = connect_to_mongodb(new_collection_name)
        new_collection.insert_many(query_output)
        logger.info(f"Query output saved to new collection '{new_collection_name}' successfully!")

    except Exception as e:
        logger.error(f"Error occurred while saving query output: {e}")
        raise


def delete_collection(collection_name):
    """
    Deletes a collection from a MongoDB database.

    :param collection_name (str): Name of the collection to delete.
    :return str: Message indicating success or failure.
    """
    try:

        # Access the database
        client = pymongo.MongoClient(MONGO_CLIENT)
        db = client[DB_NAME]

        # Check if the collection exists
        if collection_name in db.list_collection_names():
            # Drop the collection
            db[collection_name].drop()
            return logger.info(f"Collection '{collection_name}' in database '{DB_NAME}' has been deleted.")
        else:
            logger.error(f"Collection '{collection_name}' does not exist in database '{DB_NAME}'.")
            raise
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise


# Function to insert data into MongoDB given a JSON list
def insert_data_to_mongodb(collection_name:str, json_list):
    """
    :param collection_name(str): name of the collection to insert data
    :param json_list: data to be inserted
    """
    try:
        collection = connect_to_mongodb(collection_name)

        data_to_insert = [
            json.loads(item) if isinstance(item, str) else item
            for item in json_list
        ]

        # Insert the data into MongoDB collection
        logger.info(f"Inserting data into db - {DB_NAME}, Collection - {collection_name}")
        collection.insert_many(data_to_insert)

        logger.info("Data inserted successfully into MongoDB!")
    except Exception as e:
        logger.error(f"Error occurred while inserting data: {e}")
        raise


# Function to retrieve data from MongoDB as a list of JSON objects
def get_full_data_from_mongodb(collection_name:str):
    """
    :param collection_name(str): collection to retrieve the data from
    :return list of json
    """
    logger.info(f"Getting full data from mongodb (db-{DB_NAME}, collection-{collection_name}")
    try:
        collection = connect_to_mongodb(collection_name)
        
        results = list(collection.find())
        
        
        # Convert the documents from BSON to JSON-like dictionary
        json_data = [json.dumps(doc, default=str) for doc in results]
        if json_data:
            logger.info(f"Successfully retrieved {len(json_data)} documents from collection {collection_name}.")
        else:
            logger.error(f"No data found in collection {collection_name}.")
            raise
        json_list = [json.loads(item) for item in json_data]
        return  json_list
    except Exception as e:
        logger.error(f"Error occurred while retrieving data: {e}")
        raise


def fetch_data_from_collection(collection_name):
    """
    Fetch data from a specified MongoDB collection and return it as a pandas DataFrame.

    :param collection_name: Name of the collection to fetch data from.
    :return: pandas DataFrame containing the collection data.
    """
    try:
        logger.info(f"Attempting to fetch data from collection: {collection_name}")

        # Connect to the collection (assuming connect_to_mongodb is defined elsewhere)
        collection = connect_to_mongodb(collection_name)

        # Fetch data from the collection
        data = list(collection.find())

        # Check if data is retrieved
        if data:
            logger.info(f"Successfully fetched {len(data)} records from collection: {collection_name}.")
        else:
            logger.error(f"No data found in collection: {collection_name}.")
            raise

        # Convert data to pandas DataFrame
        df = pd.DataFrame(data)
        return df

    except Exception as e:
        logger.error(f"Error occurred while fetching data from collection {collection_name}: {e}")
        raise  # Return an empty DataFrame on error


def delete_all_collections():
    """
    Delete all collections from a given MongoDB database.

    :param db_name: Name of the MongoDB database.
    :param client: MongoClient object used to connect to MongoDB.
    """
    try:
        # Connect to the database
        client = pymongo.MongoClient(MONGO_CLIENT)
        db = client[DB_NAME]

        # List all collections in the database
        collections = db.list_collection_names()

        if collections:
            logger.info(f"Found {len(collections)} collections in database {DB_NAME}. Deleting them...")

            # Loop through and drop each collection
            for collection in collections:
                db.drop_collection(collection)
                logger.info(f"Collection '{collection}' has been deleted.")
        else:
            logger.warning(f"No collections found in database {DB_NAME}. Nothing to delete.")

    except Exception as e:
        logger.error(f"Error occurred while deleting collections from database {DB_NAME}: {e}")
        raise
