from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, count
import json
import logging

logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("CSV Validation Example") \
    .getOrCreate()


EV_COLUMNS_TO_DROP = [
    'Clean Alternative Fuel Vehicle (CAFV) Eligibility',
    'Base MSRP',
    'Legislative District',
    'Vehicle Location',
    'Electric Utility',
    '2020 Census Tract'
]

def drop_columns(df, columns_to_drop):
    logger.info("Dropping the following columns")
    df = df.drop(*columns_to_drop)
    return df

def filter_ev_relevant_data(df):
    logger.info("Filtering ev relevant data")
    df = df.filter(df['County'].isNotNull())
    df_filtered = df.filter(df['Model Year'] >= 2010)
    return df_filtered

def rename_columns(df, rename_dict):
    for key in rename_dict.keys():
        df = df.withColumnRenamed(key, rename_dict[key])
    return df

def apply_transformations_to_ev_data(json_list:list[dict]):
    logger.info("Converting data to PySpark DataFrame for fast-distributed computing")
    df = spark.createDataFrame(json_list)
    df = drop_columns(df, EV_COLUMNS_TO_DROP)
    filtered_df = filter_ev_relevant_data(df)
    df_renamed = rename_columns(filtered_df, {'VIN (1-10)':'VID'})
    df_transformed = df_renamed.withColumn('Postal Code', col('Postal Code').cast('int'))
    logger.info(f"Transformation completed! Total rows {df_transformed.count()}")
    transformed_data = convert_df_to_json_list(df_transformed, primary_key='DOL Vehicle ID')
    return transformed_data


def convert_df_to_json_list(df, primary_key=None):
    """
    Convert a PySpark DataFrame to a list of JSON strings and optionally set a primary key.

    Parameters:
        df (pyspark.sql.DataFrame): The PySpark DataFrame to convert.
        primary_key (str, optional): The column name to set as the "_id" field in MongoDB.

    Returns:
        list: A list of JSON strings ready for MongoDB insertion.

    Raises:
        ValueError: If the primary key is not present in the DataFrame records.
    """
    try:
        logger.info("Starting conversion of DataFrame to JSON list.")

        # Convert PySpark DataFrame to Pandas DataFrame
        pandas_df = df.toPandas()
        logger.info(f"DataFrame converted to Pandas. Shape: {pandas_df.shape}")

        # Convert Pandas DataFrame to a list of dictionaries
        json_list = pandas_df.to_dict(orient="records")
        logger.info(f"DataFrame converted to list of dictionaries with {len(json_list)} records.")

        # Check and set the primary key as "_id"
        if primary_key is not None:
            if primary_key not in pandas_df.columns:
                error_message = f"Primary key '{primary_key}' not found in DataFrame columns: {list(pandas_df.columns)}"
                logger.error(error_message)
                raise ValueError(error_message)

            for record in json_list:
                if primary_key not in record:
                    error_message = f"Primary key '{primary_key}' not found in record: {record}"
                    logger.error(error_message)
                    raise ValueError(error_message)
                record["_id"] = record[primary_key]  # Set "_id" using the primary key

            logger.info(f"Primary key '{primary_key}' successfully set as '_id' in all records.")

        # Convert each record to a JSON string
        json_strings = [json.dumps(record) for record in json_list]
        logger.info("Conversion to JSON list completed successfully.")
        return json_strings

    except ValueError as ve:
        logger.error(f"ValueError occurred: {ve}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise