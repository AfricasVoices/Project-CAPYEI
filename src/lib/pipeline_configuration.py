import json

from core_data_modules.cleaners import Codes
from core_data_modules.data_models import Scheme, validators
from dateutil.parser import isoparse


def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return Scheme.from_firebase_map(firebase_map)

class CodeSchemes(object):
    VALUABLE = _open_scheme("Valuable.json")
    CHANGE = _open_scheme("Change.json")

class CodingPlan(object):
    def __init__(self, raw_field, coded_field, coda_filename, cleaner=None, code_scheme=None, time_field=None,
                 run_id_field=None, icr_filename=None, analysis_file_key=None, id_field=None,
                 binary_code_scheme=None, binary_coded_field=None, binary_analysis_file_key=None
                 thematic_analysis_filename=None, prev_thematic_analysis_filename=None):
        self.raw_field = raw_field
        self.coded_field = coded_field
        self.coda_filename = coda_filename
        self.icr_filename = icr_filename
        self.thematic_analysis_filename = thematic_analysis_filename
        self.prev_thematic_analysis_filename = prev_thematic_analysis_filename
        self.cleaner = cleaner
        self.code_scheme = code_scheme
        self.time_field = time_field
        self.run_id_field = run_id_field
        self.analysis_file_key = analysis_file_key
        self.binary_code_scheme = binary_code_scheme
        self.binary_coded_field = binary_coded_field
        self.binary_analysis_file_key = binary_analysis_file_key

        if id_field is None:
            id_field = "{}_id".format(self.raw_field)
        self.id_field = id_field

class PipelineConfiguration(object):
    DEV_MODE = False
    
    PROJECT_START_DATE = isoparse("2019-03-29T00:00:00+03:00")
    PROJECT_END_DATE = isoparse("2019-03-30T24:00:00+03:00")

    SURVEY_CODING_PLANS = [
        CodingPlan(raw_field="capyei_valuable_raw",
                   coded_field="capyei_valuable_coded",
                   time_field="capyei_valuable_time",
                   coda_filename="capyei_valuable.json",
                   icr_filename="capyei_valuable.csv",
                   thematic_analysis_filename="capyei_valuable_thematic_new.csv",
                   prev_thematic_analysis_filename="capyei_valuable_thematic.csv",
                   analysis_file_key="capyei_valuable_",
                   cleaner=None,
                   code_scheme=CodeSchemes.VALUABLE),

        CodingPlan(raw_field="capyei_change_raw",
                   coded_field="capyei_change_coded",
                   time_field="capyei_change_time",
                   coda_filename="capyei_change.json",
                   icr_filename="capyei_change.csv",
                   thematic_analysis_filename="capyei_change_thematic_new.csv",
                   prev_thematic_analysis_filename="capyei_change_thematic.csv",
                   analysis_file_key="capyei_change_",
                   cleaner=None,
                   code_scheme=CodeSchemes.CHANGE)
    ]

    DEMOGS = [
        CodingPlan(raw_field="course_name", coded_field=None, coda_filename=None),
        CodingPlan(raw_field="Sex", coded_field=None, coda_filename=None),
        CodingPlan(raw_field="age", coded_field=None, coda_filename=None),
        CodingPlan(raw_field="training_center", coded_field=None, coda_filename=None),
    ]

    def __init__(self, rapid_pro_domain, rapid_pro_token_file_url, rapid_pro_key_remappings):
        """
        :param rapid_pro_domain: URL of the Rapid Pro server to download data from.
        :type rapid_pro_domain: str
        :param rapid_pro_token_file_url: GS URL of a text file containing the authorisation token for the Rapid Pro
                                         server.
        :type rapid_pro_token_file_url: str
        :param rapid_pro_key_remappings: List of rapid_pro_key -> pipeline_key remappings.
        :type rapid_pro_key_remappings: list of RapidProKeyRemapping
        """
        self.rapid_pro_domain = rapid_pro_domain
        self.rapid_pro_token_file_url = rapid_pro_token_file_url
        self.rapid_pro_key_remappings = rapid_pro_key_remappings
        
        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        rapid_pro_domain = configuration_dict["RapidProDomain"]
        rapid_pro_token_file_url = configuration_dict["RapidProTokenFileURL"]

        rapid_pro_key_remappings = []
        for remapping_dict in configuration_dict["RapidProKeyRemappings"]:
            rapid_pro_key_remappings.append(RapidProKeyRemapping.from_configuration_dict(remapping_dict))

        return cls(rapid_pro_domain, rapid_pro_token_file_url, rapid_pro_key_remappings)

    @classmethod
    def from_configuration_file(cls, f):
        return cls.from_configuration_dict(json.load(f))
    
    def validate(self):
        validators.validate_string(self.rapid_pro_domain, "rapid_pro_domain")
        validators.validate_string(self.rapid_pro_token_file_url, "rapid_pro_token_file_url")

        validators.validate_list(self.rapid_pro_key_remappings, "rapid_pro_key_remappings")
        for i, remapping in enumerate(self.rapid_pro_key_remappings):
            assert isinstance(remapping, RapidProKeyRemapping), \
                f"self.rapid_pro_key_mappings[{i}] is not of type RapidProKeyRemapping"
            remapping.validate()

class RapidProKeyRemapping(object):
    def __init__(self, rapid_pro_key, pipeline_key):
        """
        :param rapid_pro_key: Name of key in the dataset exported via RapidProTools.
        :type rapid_pro_key: str
        :param pipeline_key: Name to use for that key in the rest of the pipeline.
        :type pipeline_key: str
        """
        self.rapid_pro_key = rapid_pro_key
        self.pipeline_key = pipeline_key
        
        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        rapid_pro_key = configuration_dict["RapidProKey"]
        pipeline_key = configuration_dict["PipelineKey"]
        
        return cls(rapid_pro_key, pipeline_key)
    
    def validate(self):
        validators.validate_string(self.rapid_pro_key, "rapid_pro_key")
        validators.validate_string(self.pipeline_key, "pipeline_key")
