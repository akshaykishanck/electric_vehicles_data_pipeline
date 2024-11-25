import argparse
import json
import logging

from pipeline.download import download_file
from pipeline.transform import *
from pipeline.visualization import *
from utils.mongo import *
from utils.queries import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, etc.)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),  # Log to a file
        logging.StreamHandler()  # Log to the console
    ]
)

logger = logging.getLogger(__name__)
logger.info("Starting the pipeline execution...")

# Path to the checkpoint file
CHECKPOINT_FILE = "pipeline_checkpoint.json"


def load_checkpoint():
    """
    Load the checkpoint from the JSON file.

    :return: Dictionary containing the checkpoint data or None if no checkpoint exists.
    """
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            checkpoint = json.load(f)
            logger.info("Checkpoint loaded successfully.")
            return checkpoint
    except FileNotFoundError:
        logger.warning("No checkpoint file found. Starting pipeline from scratch.")
        return None
    except Exception as e:
        logger.error(f"Error loading checkpoint: {e}")
        raise

def save_checkpoint(stage):
    """
    Save the current pipeline stage to the checkpoint file.

    :param stage: The name of the stage to save.
    :return: None
    """
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump({"current_stage": stage}, f)
            logger.info(f"Checkpoint saved for stage: {stage}")
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")
        raise

def reset_checkpoint():
    """
    Reset the checkpoint by deleting the checkpoint file.

    :return: None
    """
    try:
        import os
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            logger.info("Checkpoint file reset successfully.")
    except Exception as e:
        logger.error(f"Error resetting checkpoint: {e}")
        raise

def main(reset_and_start=True):
    """
    Main function for running the pipeline.

    :param reset_and_start: Boolean flag to indicate whether to reset the pipeline and start from scratch.
    :return: None
    """
    if reset_and_start:
        logger.info("Resetting pipeline and starting from scratch.")
        delete_all_collections()
        reset_checkpoint()
        current_stage = None  # Start from the beginning
    else:
        # Load the checkpoint if it exists
        checkpoint = load_checkpoint()
        current_stage = checkpoint["current_stage"] if checkpoint else None

    # Pipeline stages
    pipeline_stages = ["download", "transform", "analyze", "visualization"]

    # Determine the starting point
    if current_stage:
        start_index = pipeline_stages.index(current_stage) + 1
    else:
        start_index = 0

    # Run the pipeline
    for i in range(start_index, len(pipeline_stages)):
        stage = pipeline_stages[i]
        logger.info(f"Running {stage}...")

        try:
            # Replace with actual function calls for each stage
            if stage == "download":
                run_download_stage()
            elif stage == "transform":
                run_transform_stage()
            elif stage == "analyze":
                run_analyze_stage()
            elif stage == "visualization":
                run_visualization_stage()

            # Save progress to the checkpoint
            save_checkpoint(stage)

        except Exception as e:
            logger.error(f"Pipeline failed at {stage} with error: {e}")
            break

def run_download_stage():
    logger.info("Executing stage 1...Download...")
    download_url = "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv"
    raw_data = download_file(download_url)
    insert_data_to_mongodb(collection_name='ev_vehicle_data_raw', json_list=raw_data)

def run_transform_stage():
    logger.info("Executing stage 2...Transform...")
    raw_json = get_full_data_from_mongodb(collection_name='ev_vehicle_data_raw')
    transformed_data = apply_transformations_to_ev_data(raw_json)
    insert_data_to_mongodb(collection_name='ev_vehicle_data_cleaned', json_list=transformed_data)
    save_checkpoint("transform")

def run_analyze_stage():
    logger.info("Executing stage 3...Analyze...")
    collection = connect_to_mongodb("ev_vehicle_data_cleaned")
    query1 = query_vehicles_by_year_and_type(collection)
    query2 = query_vehicles_by_county_and_year(collection)
    query3 = query_vehicles_by_make_and_year(collection)
    save_query_output_to_new_collection(query1, "vehicles_per_type_year")
    save_query_output_to_new_collection(query2, "vehicles_per_county_year")
    save_query_output_to_new_collection(query3, "vehicles_per_make_year")

def run_visualization_stage():
    logger.info("Executing stage 4...Visualization...")
    vehicles_by_year_and_type = fetch_data_from_collection("vehicles_per_type_year")
    vehicles_by_county_and_year = fetch_data_from_collection("vehicles_per_county_year")
    vehicles_by_make_and_year = fetch_data_from_collection("vehicles_per_make_year")

    # Generate visualizations
    plot_ev_increase_by_year(vehicles_by_year_and_type)
    plot_counties_with_most_and_least_evs(vehicles_by_county_and_year)
    plot_most_popular_make_by_year(vehicles_by_make_and_year)

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the pipeline.")
    # Add a nullable boolean argument
    parser.add_argument(
        "--reset_and_start",
        nargs="?",
        const=True,  # If the argument is provided without a value, set it to True
        default=False,  # If the argument is not provided, set it to False
        type=lambda x: x.lower() == "true" if isinstance(x, str) else bool(x),  # Handle True/False as strings
        help="Start the pipeline from scratch. Defaults to False if not provided.",
    )
    args = parser.parse_args()
    logger.info(f"Reset and Start {args.reset_and_start}")
    main(args.reset_and_start)
