"""Stage for performing frequency filtering on corpora.
"""
from base_stage import BaseStage
from configuration import run_configuration

import constants

from os.path import join
from collections import Counter

import json
import logging


def get_tokens_from_file(file_path):
    """Helper function for getting tokens from file.

    Args:
        file_path: a path to the file.

    Returns:
        A list of tokens.
    """
    with open(file_path) as file:
        text = file.read()
        tokens = text.split(" ")
    return tokens

def save_tokens_to_file(tokens, file_path):
    """Helper function for saving tokens to file.

    Args:
        tokens: a list of tokens.
        file_path: a path to the file.
    """
    with open(file_path, "w") as file:
        text = " ".join([str(t) for t in tokens])
        file.write(text)

def filter_unk(tokens, dictionary):
    """Changes things that are not in the mapping to <<unk>>

    Args:
        tokens: a list of tokens to change
        dictionary: a token to number mapping
    """
    for i, token in enumerate(tokens):
        if not token in dictionary:
            tokens[i] = "<<unk>>"

def apply_dictionary(tokens, dictionary):
    """Applies dictionary to tokens.

    Args:
        tokens: a list of tokens.
        dictionary: token to number mapping.

    Returns:
        A list of numbers generated.
    """
    return [dictionary[t] for t in tokens]

class FrequencyFilteringStage(BaseStage):
    """Stage for frequency filtering corpora.
    """
    name = "frequency_filtering"
    logger = logging.getLogger("pipeline").getChild("frequency_filtering_stage")

    def __init__(self, parent=None, frequency_threshold=0):
        """Initialization for corpus analysis stage.

        Args:
            parent: The parent stage.
            frequency_threshold: minimum number of times a token has to appear.
        """
        super(FrequencyFilteringStage, self).__init__(parent)
        self.frequency_threshold = frequency_threshold

    def pre_run(self):
        """The function that is executed before the stage is run.

        Args:
            args: command line arguments that are passed to the stage.
        """
        self.logger.info("=" * 40)
        self.logger.info("Executing frequency filtering stage.")
        self.logger.info("Frequency Threshold: {}".format(self.frequency_threshold))
        self.logger.info("-" * 40)

    def generate_dictionary(self, tokens):
        """Generates dictionary from given tokens.

        Args:
            tokens: a list of tokens.

        Returns:
            A dictionary that maps token to a number
        """
        counter = Counter(tokens)
        dictionary = {}
        id = 0
        for token in counter:
            if counter[token] > self.frequency_threshold:
                dictionary[token] = id
                id += 1
        dictionary["<<unk>>"] = id
        return dictionary


    def run(self):
        """Run analysis on the corpus file.

        Returns:
            True if the stage execution succeded, False otherwise.
        """
        self.logger.info("Starting frequency filtering...")
        train_file_path = join(constants.TMP_PATH, "{}.train.txt".format(self.parent.topic))
        test_file_path = join(constants.TMP_PATH, "{}.test.txt".format(self.parent.topic))
        valid_file_path = join(constants.TMP_PATH, "{}.valid.txt".format(self.parent.topic))

        train_tokens = get_tokens_from_file(train_file_path)
        test_tokens = get_tokens_from_file(test_file_path)
        valid_tokens = get_tokens_from_file(valid_file_path)

        self.logger.info("Generating dictionary from training tokens...")
        dictionary = self.generate_dictionary(train_tokens)
        self.logger.info("Filtering tokens that are not in the dictionary...")
        filter_unk(train_tokens, dictionary)
        filter_unk(test_tokens, dictionary)
        filter_unk(valid_tokens, dictionary)

        self.logger.info("Saving tokens to file...")
        train_file_path = join(constants.TMP_PATH, "{}.train.f.txt".format(self.parent.topic))
        test_file_path = join(constants.TMP_PATH, "{}.test.f.txt".format(self.parent.topic))
        valid_file_path = join(constants.TMP_PATH, "{}.valid.f.txt".format(self.parent.topic))
        save_tokens_to_file(train_tokens, train_file_path)
        save_tokens_to_file(test_tokens, test_file_path)
        save_tokens_to_file(valid_tokens, valid_file_path)

        self.logger.info("Converting and saving number representation...")
        train_file_path = join(constants.DATA_PATH, "{}.train.txt".format(self.parent.topic))
        test_file_path = join(constants.DATA_PATH, "{}.test.txt".format(self.parent.topic))
        valid_file_path = join(constants.DATA_PATH, "{}.valid.txt".format(self.parent.topic))
        save_tokens_to_file(apply_dictionary(train_tokens, dictionary), train_file_path)
        save_tokens_to_file(apply_dictionary(test_tokens, dictionary), test_file_path)
        save_tokens_to_file(apply_dictionary(valid_tokens, dictionary), valid_file_path)

        self.logger.info("Saving dictionary...")
        dictionary_file_path = join(constants.DATA_PATH,
                                    "{}.dictionary.json".format(self.parent.topic))
        with open(dictionary_file_path, 'w') as file:
            text = json.dumps(dictionary)
            file.write(text)
        return True
