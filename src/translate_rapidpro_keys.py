from datetime import datetime

import pytz
from core_data_modules.traced_data import Metadata
from core_data_modules.util import TimeUtils
from dateutil.parser import isoparse


class TranslateRapidProKeys(object):
    # TODO: Move the constants in this file to configuration json
    SHOW_ID_MAP = {
        "Capyei_Valuable (Value) - capyei_pp": 1,
        "Capyei_Change (Value) - capyei_pp": 1
    }

    RAW_ID_MAP = {
        1: "capyei_valuable_raw",
        1: "capyei_change_raw"
    }

    @classmethod
    def set_show_ids(cls, user, data, show_id_map):
        """
        Sets a show_id for each message, using the presence of Rapid Pro value keys to determine which show each message
        belongs to.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to set the show ids of.
        :type data: iterable of TracedData
        :param show_id_map: Dictionary of Rapid Pro value key to show id.
        :type show_id_map: dict of str -> int
        """
        for td in data:
            show_dict = dict()

            for message_key, show_id in show_id_map.items():
                if message_key in td:
                    show_dict["rqa_message"] = td[message_key]
                    show_dict["show_id"] = show_id

            td.append_data(show_dict, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    @classmethod
    def remap_key_names(cls, user, data, pipeline_configuration):
        """
        Remaps key names.
        
        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to remap the key names of.
        :type data: iterable of TracedData
        :param pipeline_configuration: Pipeline configuration.
        :type pipeline_configuration: PipelineConfiguration
        """
        for td in data:
            remapped = dict()
               
            for remapping in pipeline_configuration.rapid_pro_key_remappings:
                old_key = remapping.rapid_pro_key
                new_key = remapping.pipeline_key
                
                if old_key in td and new_key not in td:
                    remapped[new_key] = td[old_key]

            td.append_data(remapped, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    @classmethod
    def set_rqa_raw_keys_from_show_ids(cls, user, data, raw_id_map):
        """
        Despite the earlier phases of this pipeline stage using a common 'rqa_message' field and then a 'show_id'
        field to identify which radio show a message belonged to, the rest of the pipeline still uses the presence
        of a raw field for each show to determine which show a message belongs to. This function translates from
        the new 'show_id' method back to the old 'raw field presence` method.
        
        TODO: Update the rest of the pipeline to use show_ids, and/or perform remapping before combining the datasets.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to set raw radio show message fields for.
        :type data: iterable of TracedData
        :param raw_id_map: Dictionary of show id to the rqa message key to assign each td[rqa_message} to.
        :type raw_id_map: dict of int -> str
        """
        for td in data:
            for show_id, message_key in raw_id_map.items():
                if "rqa_message" in td and td.get("show_id") == show_id:
                    td.append_data({message_key: td["rqa_message"]},
                                   Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    @classmethod
    def translate_rapid_pro_keys(cls, user, data, pipeline_configuration, coda_input_dir):
        """
        Remaps the keys of rqa messages in the wrong flow into the correct one, and remaps all Rapid Pro keys to
        more usable keys that can be used by the rest of the pipeline.
        
        TODO: Break this function such that the show remapping phase happens in one class, and the Rapid Pro remapping
              in another?
        """
        # Set a show id field for each message, using the presence of Rapid Pro value keys in the TracedData.
        # Show ids are necessary in order to be able to remap radio shows and key names separately (because data
        # can't be 'deleted' from TracedData).
        cls.set_show_ids(user, data, cls.SHOW_ID_MAP)

        # Remap the keys used by Rapid Pro to more usable key names that will be used by the rest of the pipeline.
        cls.remap_key_names(user, data, pipeline_configuration)

        # Convert from the new show id format to the raw field format still used by the rest of the pipeline.
        cls.set_rqa_raw_keys_from_show_ids(user, data, cls.RAW_ID_MAP)

        return data
