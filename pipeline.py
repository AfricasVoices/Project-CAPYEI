import argparse
import os
import random

from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import PhoneNumberUuidTable, IOUtils
from storage.google_drive import drive_client_wrapper

from src.lib.pipeline_configuration import PipelineConfiguration
from src.auto_code_surveys import AutoCodeSurveys
from src.apply_manual_codes import ApplyManualCodes
from src.combine_raw_datasets import CombineRawDatasets
from src.translate_rapidpro_keys import TranslateRapidProKeys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runs the post-fetch phase of the CAPYEI pipeline",
                                     # Support \n and long lines
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("--drive-upload", nargs=4,
                        metavar=("drive-credentials-path", "csv-by-message-drive-path",
                                 "csv-by-individual-drive-path", "production-csv-drive-path"),
                        help="Upload message csv, individual csv, and production csv to Drive. Parameters:\n"
                             "  drive-credentials-path: Path to a G Suite service account JSON file\n"
                             "  csv-by-message-drive-path: 'Path' to a file in the service account's Drive to "
                             "upload the messages CSV to\n"
                             "  csv-by-individual-drive-path: 'Path' to a file in the service account's Drive to "
                             "upload the individuals CSV to\n"
                             "  production-csv-drive-path: 'Path' to a file in the service account's Drive to "
                             "upload the production CSV to"),

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file"),
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                            "credentials bucket")

    parser.add_argument("phone_number_uuid_table_path", metavar="phone-number-uuid-table-path",
                        help="JSON file containing the phone number <-> UUID lookup table for the messages/surveys "
                             "datasets")
    parser.add_argument("s02e01_input_path", metavar="s02e01-input-path",
                        help="Path to the episode 1 raw messages JSON file, containing a list of serialized TracedData "
                             "objects")
    parser.add_argument("prev_coded_dir_path", metavar="prev-coded-dir-path",
                        help="Directory containing Coda files generated by a previous run of this pipeline. "
                             "New data will be appended to these files.")
    parser.add_argument("json_output_path", metavar="json-output-path",
                        help="Path to a JSON file to write TracedData for final analysis file to")
    parser.add_argument("icr_output_dir", metavar="icr-output-dir",
                        help="Directory to write CSV files to, each containing 200 messages and message ids for use " 
                             "in inter-code reliability evaluation")
    
    parser.add_argument("coded_dir_path", metavar="coded-dir-path",
                        help="Directory to write coded Coda files to")

    parser.add_argument("csv_by_message_output_path", metavar="csv-by-message-output-path",
                        help="Analysis dataset where messages are the unit for analysis (i.e. one message per row)")
    parser.add_argument("csv_by_individual_output_path", metavar="csv-by-individual-output-path",
                        help="Analysis dataset where respondents are the unit for analysis (i.e. one respondent "
                            "per row, with all their messages joined into a single cell)")
    parser.add_argument("production_csv_output_path", metavar="production-csv-output-path",
                        help="Path to a CSV file to write raw message and demographic responses to, for use in "
                            "radio show production")

    args = parser.parse_args()

    drive_credentials_path = None
    csv_by_message_drive_path = None
    csv_by_individual_drive_path = None
    production_csv_drive_path = None

    drive_upload = args.drive_upload is not None
    if drive_upload:
        drive_credentials_path = args.drive_upload[0]
        csv_by_message_drive_path = args.drive_upload[1]
        csv_by_individual_drive_path = args.drive_upload[2]
        production_csv_drive_path = args.drive_upload[3]

    user = args.user
    pipeline_configuration_file_path = args.pipeline_configuration_file_path

    phone_number_uuid_table_path = args.phone_number_uuid_table_path
    s02e01_input_path = args.s02e01_input_path
    
    json_output_path = args.json_output_path
    icr_output_dir = args.icr_output_dir
    prev_coded_dir_path = args.prev_coded_dir_path
    coded_dir_path = args.coded_dir_path

    message_paths = [s02e01_input_path]

    # Load the pipeline configuration file
    print("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)

    # Load phone number <-> UUID table
    print("Loading Phone Number <-> UUID Table...")
    with open(phone_number_uuid_table_path, "r") as f:
        phone_number_uuid_table = PhoneNumberUuidTable.load(f)

    # Load messages
    messages_datasets = []
    for i, path in enumerate(message_paths):
        print("Loading Episode {}/{}...".format(i + 1, len(message_paths)))
        with open(path, "r") as f:
            messages_datasets.append(TracedDataJsonIO.import_json_to_traced_data_iterable(f))
    
    # Add survey data to the messages
    print("Combining Datasets...")
    data = CombineRawDatasets.combine_raw_datasets(user, messages_datasets, [])

    print("Translating Rapid Pro Keys...")
    data = TranslateRapidProKeys.translate_rapid_pro_keys(user, data, pipeline_configuration, prev_coded_dir_path)
    
    print("Auto Coding Surveys...")
    data = AutoCodeSurveys.auto_code_surveys(user, data, icr_output_dir, coded_dir_path)

    print("Applying Manual Codes from Coda...")
    data = ApplyManualCodes.apply_manual_codes(user, data, prev_coded_dir_path)

    print("Writing TracedData to file...")
    IOUtils.ensure_dirs_exist_for_file(json_output_path)
    with open(json_output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(data, f, pretty_print=True)

    print("Python script complete")
