import requests
import json
import urllib3
import re
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Suppress only the single InsecureRequestWarning from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants for API URLs and Authorization
LICENSE_BASE_URL = os.getenv('LICENSE_BASE_URL', 'https://prodam.redisdemo.com:9443/v1/license')
METRICS_URL = os.getenv('METRICS_URL', 'https://prodam.redisdemo.com:8070/metrics')
BDBS_URL = os.getenv('BDBS_URL', 'https://prodam.redisdemo.com:9443/v1/bdbs')
AUTHORIZATION = os.getenv('AUTHORIZATION', 'Basic <...>')


def get_json_response(url):
    """
    Fetches a JSON response from the given URL using the specified authorization header.

    :param url: The API URL to fetch data from.
    :return: Parsed JSON data as a Python dictionary.
    """
    logging.info(f"Fetching JSON data from {url}")
    response = requests.get(url, headers={"Authorization": AUTHORIZATION}, verify=False)
    response.raise_for_status()  # Raise an error if the request fails
    return response.json()


def parse_metrics(data, metric_name):
    """
    Parses metrics data using a regular expression to extract metric values associated with database IDs.

    :param data: Raw metrics data as a string.
    :param metric_name: The specific metric name to parse.
    :return: A dictionary mapping database IDs to metric values.
    """
    logging.info(f"Parsing metrics for {metric_name}")
    # Corrected regular expression to match only the required two groups
    pattern = re.compile(rf'{metric_name}\{{.*?bdb="(\d+)",.*?\}}\s+([\d.e+-]+)')
    matches = pattern.findall(data)
    return {db: float(value) for db, value in matches}


def fetch_license_data():
    """
    Fetches license data including cluster name, activation and expiration dates, and shard usage.

    :return: A dictionary containing the extracted license information.
    """
    license_data = get_json_response(LICENSE_BASE_URL)

    # Extract relevant fields from license data
    cluster_name = license_data.get("cluster_name", "N/A")
    activation_date = license_data.get("activation_date", "N/A")
    expiration_date = license_data.get("expiration_date", "N/A")
    expired = license_data.get("expired", "N/A")
    shards_limit = license_data.get("shards_limit", 0)
    ram_shards_in_use = license_data.get("ram_shards_in_use", 0)
    flash_shards_in_use = license_data.get("flash_shards_in_use", 0)

    # Calculate percentage of shards used
    total_shards_in_use = ram_shards_in_use + flash_shards_in_use
    percent_shards_used = (
        (total_shards_in_use / shards_limit) * 100 if shards_limit > 0 else "N/A"
    )

    # Log license information
    logging.info(f"Cluster Name: {cluster_name}")
    logging.info(f"Activation Date: {activation_date}")
    logging.info(f"Expiration Date: {expiration_date}")
    logging.info(f"Expired: {expired}")
    logging.info(f"Shards Limit: {shards_limit}")
    logging.info(f"RAM Shards In Use: {ram_shards_in_use}")
    logging.info(f"Flash Shards In Use: {flash_shards_in_use}")
    logging.info(f"Percentage of Shards Used: {percent_shards_used:.2f}%")

    return {
        "cluster_name": cluster_name,
        "activation_date": activation_date,
        "expiration_date": expiration_date,
        "expired": expired,
        "shards_limit": shards_limit,
        "ram_shards_in_use": ram_shards_in_use,
        "flash_shards_in_use": flash_shards_in_use,
        "percentage_shards_used": percent_shards_used
    }


def fetch_bdbs_data():
    """
    Fetches data for all databases (BDBs) and maps them by their unique identifiers.

    :return: A dictionary mapping database IDs to their respective BDB data.
    """
    logging.info("Fetching BDBs data")
    bdbs_data = get_json_response(BDBS_URL)
    return {str(bdb["uid"]): bdb for bdb in bdbs_data}


def fetch_metrics_data():
    """
    Fetches raw metrics data from the Prometheus endpoint.

    :return: Raw metrics data as a string.
    """
    logging.info("Fetching metrics data")
    response = requests.get(METRICS_URL, verify=False)
    response.raise_for_status()
    return response.text


def main():
    try:
        # Fetch license data
        license_info = fetch_license_data()

        # Fetch BDBs data
        bdbs_mapping = fetch_bdbs_data()

        # Fetch metrics data
        metrics_response = fetch_metrics_data()

        # Extract metrics using correct metric names
        used_memory = parse_metrics(metrics_response, 'bdb_used_memory')
        memory_limit = parse_metrics(metrics_response, 'bdb_memory_limit')
        total_keys_data = parse_metrics(metrics_response, 'redis_db_keys')

        # Output database metrics and prepare JSON
        database_list = []

        for db_id, metrics in used_memory.items():
            bdb_name = bdbs_mapping.get(db_id, {}).get("name", "N/A")
            shards_count = bdbs_mapping.get(db_id, {}).get("shards_count", "N/A")

            # Convert bytes to megabytes for clarity
            memory_used_mb = used_memory.get(db_id, 0) / (1024 * 1024)
            memory_total_mb = memory_limit.get(db_id, 1) / (1024 * 1024)  # Avoid division by zero
            total_keys = int(total_keys_data.get(db_id, 0))

            percent_memory_used = (memory_used_mb / memory_total_mb) * 100 if memory_total_mb > 0 else 0

            # Log database information
            logging.info("------------------------------------------------")
            logging.info(f"Database ID: {db_id}")
            logging.info(f"BDB Name: {bdb_name}")
            logging.info(f"Shards Count: {shards_count}")
            logging.info(f"Memory Used: {memory_used_mb:.2f} MB")
            logging.info(f"Total Memory: {memory_total_mb:.2f} MB")
            logging.info(f"Memory Usage Percentage: {percent_memory_used:.2f}%")
            logging.info(f"Total Keys: {total_keys}")
            logging.info("------------------------------------------------")

            # Prepare data for JSON
            database_list.append({
                "database_id": db_id,
                "bdb_name": bdb_name,
                "shards_count": shards_count,
                "memory_used_mb": memory_used_mb,
                "total_memory_mb": memory_total_mb,
                "memory_usage_percentage": round(percent_memory_used, 2),
                "total_keys": total_keys
            })

        # Output JSON to STDOUT
        output_json = {
            **license_info,
            "databases": database_list
        }

        print(json.dumps(output_json, indent=4))

    except Exception as e:
        logging.error("An error occurred during execution", exc_info=True)


if __name__ == "__main__":
    main()
