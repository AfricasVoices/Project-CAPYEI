from dateutil.parser import isoparse


# TODO: Move to Core once adapted for and tested on a pipeline that supports multiple radio shows
class MessageFilters(object):
    # TODO: Log which data is being dropped?
    @staticmethod
    def filter_test_messages(messages, test_run_key="test_run"):
        return [td for td in messages if not td.get(test_run_key, False)]

    @staticmethod
    def filter_empty_messages(messages, message_keys):
        # TODO: Before using on future projects, consider whether messages which are "" should be considered as empty
        non_empty = []
        for td in messages:
            for message_key in message_keys:
                if message_key in td:
                    non_empty.append(td)
                    continue
        return non_empty

    @staticmethod
    def filter_time_range(messages, time_key, start_time, end_time):
        return [td for td in messages if start_time <= isoparse(td.get(time_key)) <= end_time]

    @staticmethod
    def filter_noise(messages, message_key, noise_fn):
        return [td for td in messages if not noise_fn(td.get(message_key))]
