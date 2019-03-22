import argparse
import json
import os
import subprocess
from urllib.parse import urlparse

from google.cloud import storage

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetches all the raw data for this project from Textit. "
                                                 "This script must be run from its parent directory.")

    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file"),
    parser.add_argument("rapid_pro_tools_dir", metavar="rapid-pro-tools-dir",
                        help="Path to a directory to checkout the required version of RapidProTools to")
    parser.add_argument("root_data_dir", metavar="root-data-dir",
                        help="Path to the root of the project data directory")

    args = parser.parse_args()

    user = args.user
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    rapid_pro_tools_dir = args.rapid_pro_tools_dir
    root_data_dir = os.path.abspath(args.root_data_dir)

    uuid_table_path = f"{root_data_dir}/UUIDs/phone_uuids.json"

    SURVEYS = [
        "capyei_pp"
    ]

    TEST_CONTACTS_PATH = os.path.abspath("./test_contact_rapid_pro_ids.json")

    # Read the settings from the configuration file
    with open(pipeline_configuration_file_path) as f:
        pipeline_config = json.load(f)

        rapid_pro_domain = pipeline_config["RapidProDomain"]
        rapid_pro_token_file_url = pipeline_config["RapidProTokenFileURL"]

    # Download/checkout the appropriate version of RapidProTools
    exit_code = subprocess.call(["./checkout_rapid_pro_tools.sh", rapid_pro_tools_dir])
    assert exit_code == 0, f"./checkout_rapid_pro_tools.sh failed with exit_code {exit_code}"

    # Fetch the Rapid Pro Token from the Google Cloud Storage URL
    parsed_rapid_pro_token_file_url = urlparse(rapid_pro_token_file_url)
    assert parsed_rapid_pro_token_file_url.scheme == "gs", "RapidProTokenFileURL needs to be a gs URL " \
                                                           "(i.e. of the form gs://bucket-name/file)"
    bucket_name = parsed_rapid_pro_token_file_url.netloc
    blob_name = parsed_rapid_pro_token_file_url.path.lstrip("/")

    print(f"Downloading Rapid Pro token from file '{blob_name}' in bucket '{bucket_name}'...")
    storage_client = storage.Client.from_service_account_json(google_cloud_credentials_file_path)
    credentials_bucket = storage_client.bucket(bucket_name)
    credentials_file = credentials_bucket.blob(blob_name)
    rapid_pro_token = credentials_file.download_as_string().strip()
    print("Downloaded Rapid Pro token.")

    # Download all the runs for each of the surveys
    for survey in SURVEYS:
        output_file_path = f"{root_data_dir}/Raw Data/{survey}.json"
        print(f"Exporting survey '{survey}' to '{output_file_path}'...")

        exit_code = subprocess.call([
            "./docker-run.sh",
            "--flow-name", survey,
            "--test-contacts-path", TEST_CONTACTS_PATH,
            rapid_pro_domain,
            rapid_pro_token,
            user,
            "latest-only",
            uuid_table_path,
            output_file_path
        ], cwd=f"{rapid_pro_tools_dir}/fetch_runs")

        assert exit_code == 0, f"{rapid_pro_tools_dir}/fetch_runs/fetch_runs.py failed with exit_code {exit_code}"
