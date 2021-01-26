"""Stage for using SRILM model on the corpora.
"""
from base_stage import BaseStage
from configuration import run_configuration

import constants

from os.path import join

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
        tokens = [int(t) for t in text.split(" ")]
    return tokens

class SRILMModelStage(BaseStage):
    """Stage for applying SRILM model on the corpora.
    """
    name = "srilm_model"
    logger = logging.getLogger("pipeline").getChild("srilm_model_stage")

    def __init__(self, parent=None, ngram=2):
        """Initialization for SRILM model stage.

        Args:
            parent: The parent stage.
            ngram: the ngram size for the model.
        """
        super(SRILMModelStage, self).__init__(parent)
        self.ngram = ngram

    def pre_run(self):
        """The function that is executed before the stage is run.
        """
        self.logger.info("=" * 40)
        self.logger.info("Executing SRILM model stage.")
        self.logger.info("ngram: {}".format(self.ngram))
        self.logger.info("-" * 40)

    def run(self):
        """Creates and tests SRILM model from the corpora.

        Returns:
            True if the stage execution succeded, False otherwise.
        """
        self.logger.info("Starting model training...")
        train_file_path = join(constants.DATA_PATH, "{}.train.txt".format(self.parent.topic))
        test_file_path = join(constants.DATA_PATH, "{}.test.txt".format(self.parent.topic))
        valid_file_path = join(constants.DATA_PATH, "{}.valid.txt".format(self.parent.topic))
        train_tokens = get_tokens_from_file(train_file_path)
        test_tokens = get_tokens_from_file(test_file_path)
        valid_tokens = get_tokens_from_file(valid_file_path)
        return True
