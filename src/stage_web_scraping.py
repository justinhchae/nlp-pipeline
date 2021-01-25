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
    """Helper function that performs sparql query to find the articles for scrapping.

    Args:
        sparql_file_path: the path to the file that contains sparql query.

    Returns:
        Pandas data frame containing the result of the query.
    """
    with open(sparql_file_path, "r") as query_file:
        query = query_file.read()
        sparql = SPARQLWrapper(constants.WIKIDATA_URL)
        sparql.setReturnFormat(JSON)
        sparql.setQuery(query)
        results = sparql.query().convert()
        results_df = pd.json_normalize(results['results']['bindings'])
        return results_df

def scrape_article(site, query_row, min_num_tokens=500):
    """Helper function that scrapes 1 wikipedia article.

    Args:
        site: pywikibot Site.
        query_row: 1 row of the result from the sparql query.
        min_num_tokens: required minimum number of tokens in the page.

    Returns:
        String with the article, if it has more than min_num_tokens tokens.
    """
    entry_label = query_row["hLabel.value"]
    try:
        page = pywikibot.Page(site, entry_label)
        num_tokens = page.text.split("")
        if num_tokens < min_num_tokens:
            result = ""
        else:
            result = "<<article_start>> {} <<article_end>>\n".format(page.text)
    except Exception:
        result = ""
    return result

class WebScrapingStage(BaseStage):
    """Stage for scraping the data from the internet.
    """
    name = "web_scraping"
    logger = logging.getLogger("pipeline").getChild("web_scraping_stage")

    def __init__(self, parent=None, sparql_file="search_query.sparql", min_num_tokens=500):
        """Initialization for Web Scraping stage.

        Args:
            parent: The parent stage.
            sparql_file: file with sparql query for wikidata.
            min_num_tokens: The minimum number of tokens in the article.
        """
        super(WebScrapingStage, self).__init__(parent)
        self.search_query_file_path = join(constants.SQL_SCRIPTS_PATH, sparql_file)
        self.min_num_tokens = min_num_tokens

    def pre_run(self, args):
        """The function that is executed before the stage is run.

        Args:
            args: command line arguments that are passed to the stage.
        """
        self.logger.info("=" * 40)
        self.logger.info("Executing web scraping stage")
        self.logger.info("-" * 40)

    def run(self, args):
        """Scraps the articles from wikipedia.

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
                output_file.write(scrape_article(site, article_list.iloc[i], self.min_num_tokens))
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
