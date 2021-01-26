"""Stage for analysing a corpus.
"""
from base_stage import BaseStage
from configuration import run_configuration

import constants

from os.path import join

import logging


class CorpusAnalysisStage(BaseStage):
    """Stage for analyzing corpus.
    """
    name = "corpus_analysis"
    logger = logging.getLogger("pipeline").getChild("corpus_analysis_stage")

    def __init__(self, parent=None, corpus_file=None):
        """Initialization for corpus analysis stage.

        Args:
            parent: The parent stage.
            corpus_file: corpus file to analyze.
        """
        super(CorpusAnalysisStage, self).__init__(parent)
        self.corpus_file = corpus_file

    def pre_run(self):
        """The function that is executed before the stage is run.
        """
        self.logger.info("=" * 40)
        self.logger.info("Executing corpus analysis stage.")
        self.logger.info("Target file: {}".format(self.corpus_file))
        self.logger.info("-" * 40)

    def run(self):
        """Run analysis on the corpus file.

        Returns:
            True if the stage execution succeded, False otherwise.
        """
        self.logger.info("Starting analysis...")
        corpus_file_path = join(constants.TMP_PATH,
                                "{}.{}".format(self.parent.topic, self.corpus_file))

        with open(corpus_file_path, "r") as file:
            text = file.read()

        tokens = text.split(" ")
        self.logger.info("Corpus contians {} tokens".format(len(tokens)))
        return True
