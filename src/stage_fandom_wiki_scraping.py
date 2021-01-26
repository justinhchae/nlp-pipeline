"""Stage for scrapping the text data from the wikipedia.
"""
from base_stage import BaseStage
from configuration import run_configuration

import constants

from os.path import join

import logging
import pandas as pd
import requests
import pywikibot


def get_article_list(site, category):
    """Helper function that finds a list of articles to scrape.

    Args:
        site: pywikibot.Site() to use for selecting articles.
        category: The category to use for selecting articles.

    Returns:
        Pandas data frame containing the result of the query.
    """
    return None

def scrape_article(site, query_row, min_num_tokens=500):
    """Helper function that scrapes 1 fandom wiki article.

    Args:
        site: pywikibot.Site().
        query_row: 1 row of the result from the sparql query.
        min_num_tokens: required minimum number of tokens in the page.

    Returns:
        String with the article, if it has more than min_num_tokens tokens.
    """
    entry_label = query_row["hLabel.value"]
    try:
        page = pywikibot.Page(site, entry_label)
        num_tokens = len(page.text.split(" "))
        if num_tokens < min_num_tokens:
            result = ""
        else:
            result = "<<article_start>> {} <<article_end>>\n".format(page.text)
    except Exception:
        result = ""
    return result

class FandomWikiScrapingStage(BaseStage):
    """Stage for scraping the data from the fandom wiki.
    """
    name = "fandom_wiki_scraping"
    logger = logging.getLogger("pipeline").getChild("fandom_wiki_scraping_stage")

    def __init__(self, parent=None, category="Jedi_Masters_of_the_Jedi_Order",
                 fandom="starwars", min_num_tokens=500):
        """Initialization for Fandom Wiki Scraping stage.

        Args:
            parent: The parent stage.
            category: The category to scrape.
            fnadom: Which fandom to scrape.
            min_num_tokens: The minimum number of tokens in the article.
        """
        super(FandomWikiScrapingStage, self).__init__(parent)
        self.category = category
        self.fandom = fandom
        self.min_num_tokens = min_num_tokens

    def pre_run(self):
        """The function that is executed before the stage is run.
        """
        self.logger.info("=" * 40)
        self.logger.info("Executing fandom wiki scraping stage")
        self.logger.info("Scraping {} articles on the {} fandom.".format(self.category,
                                                                         self.fandom))
        self.logger.info("-" * 40)

    def run(self):
        """Scraps the articles from fandom wiki.

        Returns:
            True if the stage execution succeded, False otherwise.
        """
        self.logger.info("Looking for articles to scrape...")
        site = pywikibot.Site("en", self.fandom)
        article_list = get_article_list(site, self.category)
        self.logger.info("Got {} possible articles.".format(len(article_list)))

        step_size = len(article_list) // 10
        output_file_path = join(constants.TMP_PATH, "{}.raw.txt".format(self.parent.topic))
        with open(output_file_path, "w") as output_file:
            for i in range(len(article_list)):
                output_file.write(scrape_article(site, article_list.iloc[i], self.min_num_tokens))
                if i % step_size == step_size - 1:
                    self.logger.info("Scraped {} articles out of {}".format(i+1, len(article_list)))

        with open(output_file_path, "r") as output_file:
            num_tokens = len(output_file.read().split(" "))
            self.logger.info("Scraping finished. Output contains ~ {} tokens".format(num_tokens))
        return True
