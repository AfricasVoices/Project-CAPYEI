from core_data_modules.traced_data import TracedData


class CombineRawDatasets(object):
    @staticmethod
    def combine_raw_datasets(user, messages_datasets, surveys_datasets):
        data = []

        for messages_dataset in messages_datasets:
            data.extend(messages_dataset)

        for surveys_dataset in surveys_datasets:
            TracedData.update_iterable(user, "avf_phone_id", data, surveys_dataset, "survey_responses")

        return data
