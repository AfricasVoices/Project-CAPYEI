import random


# TODO: Move to Core
class ICRTools(object):
    @staticmethod
    def generate_sample_for_icr(data, sample_size, random_generator=None):
        # FIXME: Should data be de-duplicated before exporting for ICR?

        if random_generator is None:
            random_generator = random
        if len(data) < sample_size:
            print("Warning: The size of the ICR data ({} items) is less than the requested sample_size "
                  "({} items). Returning all the input data as ICR.".format(len(data), sample_size))
            sample_size = len(data)

        return random_generator.sample(data, sample_size)
