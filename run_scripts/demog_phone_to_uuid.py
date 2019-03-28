import argparse
import json
import os
import time

from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.traced_data.io import TracedDataCSVIO, TracedDataJsonIO
from core_data_modules.util import PhoneNumberUuidTable

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("user", help="Identifier of user launching this program, for use in TracedData Metadata")
    parser.add_argument("phone_uuid_table_path", metavar="phone-uuid-table-path",
                        help="JSON file containing an existing phone number <-> UUID lookup table. "
                             "This file will be updated with the new phone numbers which are found by this process")
    parser.add_argument("demog_dataset_path", metavar="demog-dataset-path",
                        help="CSV file containing demographics of CAPYEI students. ")
    parser.add_argument("json_output_path", metavar="json-output-path",
                        help="Path to serialized TracedData JSON file")

    args = parser.parse_args()
    user = args.user
    phone_uuid_path = args.phone_uuid_table_path
    demog_dataset_path = args.demog_dataset_path
    json_output_path = args.json_output_path

    with open(phone_uuid_path, "r") as f:
        phone_uuids = PhoneNumberUuidTable.load(f)

    with open(demog_dataset_path, "r") as f:
        traced_demog = TracedDataCSVIO.import_csv_to_traced_data_iterable(user, f)
        traced_demog = list(traced_demog)
        for td in traced_demog:
            uuid_dict = {"avf_phone_id": phone_uuids.add_phone(td["final_phone"])}
            td.append_data(uuid_dict, Metadata(user, Metadata.get_call_location(), time.time()))

    # Write the UUIDs out to a file
    with open(phone_uuid_path, "w") as f:
        phone_uuids.dump(f)
    
    # Output TracedData to JSON.
    if os.path.dirname(json_output_path) is not "" and not os.path.exists(os.path.dirname(json_output_path)):
        os.makedirs(os.path.dirname(json_output_path))
    with open(json_output_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_json(traced_demog, f, pretty_print=True)




