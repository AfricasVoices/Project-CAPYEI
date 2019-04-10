import time
import os
import random
import csv

from core_data_modules.cleaners import Codes, PhoneCleaner
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCSVIO, TracedDataCodaV2IO
from core_data_modules.util import IOUtils

from src.lib.pipeline_configuration import PipelineConfiguration
from src.lib.icr_tools import ICRTools


class AutoCodeSurveys(object):
    SENT_ON_KEY = "sent_on"
    ICR_MESSAGES_COUNT = 250
    ICR_SEED = 0

    CODA_CODE_PLANS = []
    CODA_CODE_PLANS.extend(PipelineConfiguration.SINGLE_CODING_PLANS)
    CODA_CODE_PLANS.extend(PipelineConfiguration.MULTI_CODING_PLANS)

    @classmethod
    def auto_code_surveys(cls, user, data, icr_output_dir, coda_output_dir, prev_coded_dir):
        # Auto-code surveys
        for plan in cls.CODA_CODE_PLANS:
            if plan.cleaner is not None:
                CleaningUtils.apply_cleaner_to_traced_data_iterable(user, data, plan.raw_field, plan.coded_field,
                                                                    plan.cleaner, plan.code_scheme)

        # Output answers to coda for manual verification + coding
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in cls.CODA_CODE_PLANS:
            
            TracedDataCodaV2IO.compute_message_ids(user, data, plan.raw_field, plan.id_field)

            coda_output_path = os.path.join(coda_output_dir, plan.coda_filename)
            with open(coda_output_path, "w") as f:
                TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                    data, plan.raw_field, plan.time_field, plan.id_field, {plan.coded_field: plan.code_scheme}, f
                )
           
        # Output messages for thematic analysis
        IOUtils.ensure_dirs_exist(icr_output_dir)
        for plan in cls.CODA_CODE_PLANS:
            prev_thematic_analysis_input_path = os.path.join(prev_coded_dir, plan.prev_thematic_analysis_filename)
            if os.path.isfile(prev_thematic_analysis_input_path):
                with open(prev_thematic_analysis_input_path, "r") as f:
                    prev_thematic_analysis_dict = csv.DictReader(f)
                    prev_thematic_analysis_messages = [message[plan.raw_field] for message in prev_thematic_analysis_dict]
                    rqa_messages = []
                    for td in data:
                        if plan.raw_field in td:
                            if td[plan.raw_field] not in prev_thematic_analysis_messages:
                                rqa_messages.append(td)
            else:
                rqa_messages = []
                for td in data:
                    if plan.raw_field in td:
                            rqa_messages.append(td)

            icr_output_path = os.path.join(icr_output_dir, plan.thematic_analysis_filename)
            with open(icr_output_path, "w") as f:
                TracedDataCSVIO.export_traced_data_iterable_to_csv(
                    rqa_messages, f, headers=[plan.raw_field]
                )
        
        # Output messages for ICR
        for plan in cls.CODA_CODE_PLANS:
            rqa_messages = []
            for td in data:
                if plan.raw_field in td:
                    rqa_messages.append(td)

            icr_messages = ICRTools.generate_sample_for_icr(
                rqa_messages, cls.ICR_MESSAGES_COUNT, random.Random(cls.ICR_SEED))

            icr_output_path = os.path.join(icr_output_dir, plan.icr_filename)
            with open(icr_output_path, "w") as f:
                TracedDataCSVIO.export_traced_data_iterable_to_csv(
                    icr_messages, f, headers=[plan.raw_field]
                )

        return data
        
