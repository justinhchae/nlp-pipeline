"""Stage for scrapping the text data from the internet.
"""
from base_stage import BaseStage
from configuration import run_configuration

import constants

import argparse
import logging


class TextCleaningStage(BaseStage):
    """Stage for cleaning text data.
    """
    name = "text_cleaning"
    logger = logging.getLogger("pipeline").getChild("text_cleaning_stage")

    def pre_run(self, args):
        """The function that is executed before the stage is run.

        Args:
            args: command line arguments that are passed to the stage.
        """
        self.logger.info("=" * 40)
        self.logger.info("Executing text cleaning stage")
        self.logger.info("Using following arguments:")
        self.logger.info("-" * 40)

    def run(self, args):
        """Downloads the db file from the url.

        Args:
            args: arguments that are passed to the stage.

        Returns:
            True if the stage execution succeded, False otherwise.
        """
        self.logger.info("Starting download")
        self.logger.info("Download finished")
        return True

    def get_argument_parser(self, use_shared_parser=False, add_help=False):
        """Returns Argument Parser to use for the stage.

        Args:
            use_shared_parser: whether to use shared parser as parent.
            add_help: whether to add help.
        """
        parser = self.get_base_argument_parser(use_shared_parser, add_help,
                                               "Download stage of the pipeline / workflow")
        #parser.add_argument("--url",
        #                    help="url for data downloading.",
        #                    default=constants.SQL_DB_FILE_URL)
        return parser
