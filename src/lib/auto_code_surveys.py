"""
import random
import time
from os import path

from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCSVIO, TracedDataCodaV2IO
from core_data_modules.util import IOUtils

from src.lib.message_filters import MessageFilters
from src.lib.icr_tools import ICRTools
from src.lib.pipeline_configuration import PipelineConfiguration

class AutoCodeShowMessages(object):
    RQA_KEYS = []
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        RQA_KEYS.append(plan.raw_field)

    ICR_MESSAGES_COUNT = 200
    ICR_SEED = 0

    @classmethod
    def auto_code_show_messages(cls, user, data, icr_output_dir, coda_output_dir):
        # Filter out test messages sent by AVF.
        if not PipelineConfiguration.DEV_MODE:
            data = MessageFilters.filter_test_messages(data)

        # Filter for runs which don't contain a response to any week's question
        data = MessageFilters.filter_empty_messages(data, cls.RQA_KEYS)

        # Output messages to Coda
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            TracedDataCodaV2IO.compute_message_ids(user, data, plan.raw_field, plan.id_field)
            for k in data:
                for value in k:
                    print(value)

            output_path = path.join(coda_output_dir, plan.coda_filename)
            with open(output_path, "w") as f:
                TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                    data, plan.raw_field, plan.time_field, plan.id_field, {}, f
                )

        # Output messages for ICR
        IOUtils.ensure_dirs_exist(icr_output_dir)
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            rqa_messages = []
            for td in data:
                # This test works because the only codes which have been applied at this point are TRUE_MISSING.
                # If any other coding is done above, this test will need to change.
                if plan.coded_field not in td:
                    rqa_messages.append(td)
                else:
                    assert len(td[plan.coded_field]) == 1
                    assert td[plan.coded_field][0]["CodeID"] == \
                        plan.code_scheme.get_code_with_control_code(Codes.TRUE_MISSING).code_id

            icr_messages = ICRTools.generate_sample_for_icr(
                rqa_messages, cls.ICR_MESSAGES_COUNT, random.Random(cls.ICR_SEED))

            icr_output_path = path.join(icr_output_dir, plan.icr_filename)
            with open(icr_output_path, "w") as f:
                TracedDataCSVIO.export_traced_data_iterable_to_csv(
                    icr_messages, f, headers=[plan.run_id_field, plan.raw_field]
                )

        return data
"""
import time
from os import path
import random

from core_data_modules.cleaners import Codes, PhoneCleaner
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCSVIO, TracedDataCodaV2IO
from core_data_modules.util import IOUtils

from src.lib.pipeline_configuration import CodeSchemes, PipelineConfiguration
from src.lib.icr_tools import ICRTools


class AutoCodeSurveys(object):
    SENT_ON_KEY = "sent_on"
    ICR_MESSAGES_COUNT = 200
    ICR_SEED = 0

    @classmethod
    def auto_code_surveys(cls, user, data, icr_output_dir, coda_output_dir):
        # Auto-code surveys
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            if plan.cleaner is not None:
                CleaningUtils.apply_cleaner_to_traced_data_iterable(user, data, plan.raw_field, plan.coded_field,
                                                                    plan.cleaner, plan.code_scheme)

        # Output single-scheme answers to coda for manual verification + coding
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            
            TracedDataCodaV2IO.compute_message_ids(user, data, plan.raw_field, plan.id_field)

            coda_output_path = path.join(coda_output_dir, plan.coda_filename)
            with open(coda_output_path, "w") as f:
                TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                    data, plan.raw_field, plan.time_field, plan.id_field, {plan.coded_field: plan.code_scheme}, f
                )
        
        # Output messages for ICR
        IOUtils.ensure_dirs_exist(icr_output_dir)
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            rqa_messages = []
            for td in data:
                # This test works because the only codes which have been applied at this point are TRUE_MISSING.
                # If any other coding is done above, this test will need to change.
                if plan.coded_field not in td:
                    rqa_messages.append(td)
                else:
                    assert len(td[plan.coded_field]) == 1
                    assert td[plan.coded_field][0]["CodeID"] == \
                        plan.code_scheme.get_code_with_control_code(Codes.TRUE_MISSING).code_id

            icr_messages = ICRTools.generate_sample_for_icr(
                rqa_messages, cls.ICR_MESSAGES_COUNT, random.Random(cls.ICR_SEED))

            icr_output_path = path.join(icr_output_dir, plan.icr_filename)
            with open(icr_output_path, "w") as f:
                TracedDataCSVIO.export_traced_data_iterable_to_csv(
                    icr_messages, f, headers=[plan.run_id_field, plan.raw_field]
                )


        return data