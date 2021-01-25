"""Stage for scrapping the text data from the internet.
"""
from base_stage import BaseStage
from configuration import run_configuration

import constants

from os.path import join

import argparse
import logging
import re


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
        self.logger.info("-" * 40)

    def run(self, args):
        """Downloads the db file from the url.

        Args:
            args: arguments that are passed to the stage.

        Returns:
            True if the stage execution succeded, False otherwise.
        """
        self.logger.info("Starting text cleaning...")
        input_file_path = join(constants.TMP_PATH, "{}.raw.txt".format(self.parent.topic))
        output_file_path = join(constants.TMP_PATH, "{}.clean.txt".format(self.parent.topic))

        with open(input_file_path, "r") as file:
            text = file.read()

        self.logger.info("Removing {{...}}")
        text = re.sub('\{{2}(.*?)\}{2}', '', text)

        self.logger.info("Removing new lines")
        text = re.sub('\n', ' <<new_line>> ', text)

        self.logger.info("Removing {{...}}")
        text = re.sub('\{{2}(.*?)\}{2}', '', text)

        self.logger.info("Removing {|...|}")
        text = re.sub('\{\|(.*?)\|\}', '', text)

        self.logger.info("Removing [[File: ...]]")
        text = re.sub('\[{2}File(.*?)\]{2}', '', text)

        self.logger.info("Removing <ref>...</ref>")
        text = re.sub('<ref(.*?)>(.*?)</ref>', '', text)

        self.logger.info("Removing <gallery>...</gallery>")
        text = re.sub('<gallery(.*?)>(.*?)</gallery>', '', text)

        self.logger.info("Opening [[...]]")
        text = re.sub('\[{2}(.*?)(\|[\w\s\|]*)?\]{2}', '\\1', text)

        self.logger.info("Removing [...]")
        text = re.sub('\[(.*?)\]', '', text)

        self.logger.info("Replacing years with <<year>>")
        text = re.sub('[\s\W]\d{4}[\s\Ws]', ' <<year>> ', text)

        self.logger.info("Refactoring possesive 's")
        text = re.sub('\'s', ' s', text)

        self.logger.info("Removing quotation marks")
        text = re.sub('[\'\"]+', ' ', text)

        self.logger.info("Section titles")
        #text = re.sub('==+(.*?)==+', ' <<title_start>> \\1 <<title_end>> ', text)
        text = re.sub('==+', ' ', text)

        self.logger.info("Removing extra spaces")
        text = re.sub('\s\s+', ' ', text)

        with open(output_file_path, "w") as file:
            file.write(text)
            num_tokens = len(text.split(" "))
            self.logger.info("Saved the cleaned text. Contains ~ {} tokens".format(num_tokens))
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
