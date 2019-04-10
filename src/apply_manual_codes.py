import time
from os import path

from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaV2IO
from core_data_modules.util import TimeUtils

from src.lib.pipeline_configuration import CodeSchemes, PipelineConfiguration


class ApplyManualCodes(object):
    @classmethod
    def apply_manual_codes(cls, user, data, coda_input_dir):
        # Merge manually coded radio show files into the cleaned dataset
        for plan in PipelineConfiguration.MULTI_CODING_PLANS:
            rqa_messages = [td for td in data if plan.raw_field in td]
            coda_input_path = path.join(coda_input_dir, plan.coda_filename)
            print(coda_input_path)

            f = None
            try:
                if path.exists(coda_input_path):
                    f = open(coda_input_path, "r")
                TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable_multi_coded(
                    user, rqa_messages, plan.id_field, {plan.coded_field: plan.code_scheme}, f)

                if plan.binary_code_scheme is not None:
                    if f is not None:
                        f.seek(0)
                    TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable(
                        user, rqa_messages, plan.id_field, {plan.binary_coded_field: plan.binary_code_scheme}, f)
            finally:
                if f is not None:
                    f.close()

        # At this point, the TracedData objects still contain messages for at most one week each.
        # Label the weeks for which there is no response as TRUE_MISSING.
        for td in data:
            missing_dict = dict()
            for plan in PipelineConfiguration.MULTI_CODING_PLANS:
                if plan.raw_field not in td:
                    na_label = CleaningUtils.make_label_from_cleaner_code(
                        plan.code_scheme, plan.code_scheme.get_code_with_control_code(Codes.TRUE_MISSING),
                        Metadata.get_call_location()
                    )
                    missing_dict[plan.coded_field] = [na_label.to_dict()]

                    if plan.binary_code_scheme is not None:
                        na_label = CleaningUtils.make_label_from_cleaner_code(
                            plan.binary_code_scheme, plan.binary_code_scheme.get_code_with_control_code(Codes.TRUE_MISSING),
                            Metadata.get_call_location()
                        )
                        missing_dict[plan.binary_coded_field] = na_label.to_dict()

            td.append_data(missing_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # Merge manually coded survey files into the cleaned dataset
        for plan in PipelineConfiguration.SINGLE_CODING_PLANS:
            f = None
            try:
                coda_input_path = path.join(coda_input_dir, plan.coda_filename)
                if path.exists(coda_input_path):
                    f = open(coda_input_path, "r")
                TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable(
                    user, data, plan.id_field, {plan.coded_field: plan.code_scheme}, f)
            finally:
                if f is not None:
                    f.close()

        # Not everyone will have answered all of the demographic flows.
        # Label demographic questions which had no responses as TRUE_MISSING.
        for td in data:
            missing_dict = dict()
            for plan in PipelineConfiguration.SINGLE_CODING_PLANS:
                if td.get(plan.raw_field, "") == "":
                    na_label = CleaningUtils.make_label_from_cleaner_code(
                        plan.code_scheme, plan.code_scheme.get_code_with_control_code(Codes.TRUE_MISSING),
                        Metadata.get_call_location()
                    )
                    missing_dict[plan.coded_field] = na_label.to_dict()
            td.append_data(missing_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        return data
