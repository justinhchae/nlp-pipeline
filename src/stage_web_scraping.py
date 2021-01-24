"""Stage for scrapping the text data from the internet.
"""
from base_stage import BaseStage
from configuration import run_configuration

import constants

from os.path import join
from SPARQLWrapper import SPARQLWrapper, JSON

import argparse
import logging
import pandas as pd
import requests
import pywikibot


def get_article_list(sparql_file_path):
    with open(sparql_file_path, "r") as query_file:
        query = query_file.read()
        sparql = SPARQLWrapper(constants.WIKIDATA_URL)
        sparql.setReturnFormat(JSON)
        sparql.setQuery(query)
        results = sparql.query().convert()
        results_df = pd.json_normalize(results['results']['bindings'])
        return results_df

def scrape_article(site, query_row):
    entry_label = query_row["hLabel.value"]
    try:
        page = pywikibot.Page(site, entry_label)
        result = "<<start_article>> {} <<end_article>>\n".format(page.text)
    except Exception:
        result = ""

    return result

class WebScrapingStage(BaseStage):
    """Stage for scraping the data from the internet.
    """
    name = "web_scraping"
    logger = logging.getLogger("pipeline").getChild("web_scraping_stage")

    def __init__(self, parent=None, sparql_file="search_query.sparql"):
        """Initialization for Web Scraping stage.

        Args:
            parent: The parent stage.
            sparql_file: file with sparql query for wikidata.
        """
        super(WebScrapingStage, self).__init__(parent)
        self.search_query_file_path = join(constants.SQL_SCRIPTS_PATH, sparql_file)

    def pre_run(self, args):
        """The function that is executed before the stage is run.

        Args:
            args: command line arguments that are passed to the stage.
        """
        self.logger.info("=" * 40)
        self.logger.info("Executing web scraping stage")
        self.logger.info("-" * 40)

    def run(self, args):
        """Downloads the db file from the url.

        Args:
            args: arguments that are passed to the stage.

        Returns:
            True if the stage execution succeded, False otherwise.
        """
        self.logger.info("Querying wikidata for articles to scrape...")
        article_list = get_article_list(self.search_query_file_path)
        self.logger.info("Got {} articles.".format(len(article_list)))

        site = pywikibot.Site("en", "wikipedia")
        step_size = len(article_list) // 10
        output_file_path = join(constants.TMP_PATH, "{}.raw.txt".format(self.parent.topic))
        with open(output_file_path, "a") as output_file:
            for i in range(len(article_list)):
                output_file.write(scrape_article(site, article_list.iloc[i]))
                if i % step_size == step_size - 1:
                    self.logger.info("Scraped {} articles out of {}".format(i+1, len(article_list)))

        with open(output_file_path, "r") as output_file:
            num_tokens = len(output_file.read().split(" "))
            self.logger.info("Scraping finished. Output countains ~ {} tokens".format(num_tokens))
        return True

    def get_argument_parser(self, use_shared_parser=False, add_help=False):
        """Returns Argument Parser to use for the stage.

        Args:
            use_shared_parser: whether to use shared parser as parent.
            add_help: whether to add help.
        """
        parser = self.get_base_argument_parser(use_shared_parser, add_help,
                                               "Web sraping stage of the pipeline / workflow")
        return parser
