"""Stage for scrapping the text data from the wikipedia.
"""
from base_stage import BaseStage
from configuration import run_configuration

import constants

from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from os.path import join
from wiki_dump_reader import Cleaner

import logging
import nltk
import re


nltk.download('punkt')
nltk.download('wordnet')

class WikipediaTextCleaningStage(BaseStage):
    """Stage for cleaning wikipedia text data.
    """
    name = "wikipedia_text_cleaning"
    logger = logging.getLogger("pipeline").getChild("wikipedia_text_cleaning_stage")

    def pre_run(self):
        """The function that is executed before the stage is run.
        """
        self.logger.info("=" * 40)
        self.logger.info("Executing text cleaning stage")
        self.logger.info("-" * 40)

    def run(self):
        """Cleans the text gotten from wikipedia.

        Returns:
            True if the stage execution succeded, False otherwise.
        """
        self.logger.info("Starting text cleaning...")
        input_file_path = join(constants.TMP_PATH, "{}.raw.txt".format(self.parent.topic))
        output_file_path = join(constants.TMP_PATH, "{}.clean.txt".format(self.parent.topic))
        cleaner = Cleaner()

        with open(input_file_path, "r") as file:
            text = file.read()

        text = re.sub('&nbsp', '', text)

        self.logger.info("Cleaning the markup and applying token-wise operations")
        lemmatizer = WordNetLemmatizer()
        articles = text.split("<<article_end>>")
        for i in range(len(articles)):
            article = articles[i]
            # Removing special tokens
            article = re.sub('<<article_start>>', '', article)
            # Removing wikipedia markup
            article = cleaner.clean_text(article)
            # Removing left out >
            article = re.sub(">", '', article)
            # Openning up [[...]]
            article = re.sub('\[{2}(.*?)(\|[\w\s\|]*)?\]{2}', '\\1', article)
            # Removing |
            article = re.sub('\|', ' ', article)

            tokens = word_tokenize(article)
            for j in range(len(tokens)):
                token = tokens[j]
                token = token.lower()
                token = token.encode("ascii", "ignore")
                token = token.decode()
                token = lemmatizer.lemmatize(token)
                tokens[j] = token
            article = " ".join(tokens)

            articles[i] = "<<article_start>> {} <<article_end>>".format(article)
        text = " ".join(articles)

        self.logger.info("Changing years to <<year>>")
        text = re.sub(' \d{4}(\-\d+|s)?', ' <<year>>', text)

        self.logger.info("Changing numbers to <<number>>")
        text = re.sub(' \d[\d\.,%]*(st|nd|rd|th| %)?', ' <<number>>', text)
        text = re.sub('<<number>>\-[\d\.,%]+', '<<number>>', text)

        self.logger.info("Section title formatting")
        text = re.sub('==+(.*?)==+', '<<section_title_start>> \\1 <<section_title_end>>', text)

        self.logger.info("Removing extra white-spaces")
        text = re.sub('\s\s+', ' ', text)

        with open(output_file_path, "w") as file:
            file.write(text)
            num_tokens = len(text.split(" "))
            self.logger.info("Saved the cleaned text. Contains ~ {} tokens".format(num_tokens))
        return True
