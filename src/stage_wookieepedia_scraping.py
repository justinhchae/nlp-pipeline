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


class Family(pywikibot.family.Family):
    name = 'starwars'
    langs = {
        'en': None,
    }
    # A few selected big languages for things that we do not want to loop over
    # all languages. This is only needed by the titletranslate.py module, so
    # if you carefully avoid the options, you could get away without these
    # for another wiki family.
    languages_by_size = ['en']
    def hostname(self,code):
        return 'starwars.wikia.com'
    def path(self, code):
        return '/index.php'
    def version(self, code):
        return "1.9" # Which version of MediaWiki is used?

def get_article_list(article_type, site):
    """Helper function that performs sparql query to find the articles for scrapping.

    Args:
        sparql_file_path: the path to the file that contains sparql query.

    Returns:
        Pandas data frame containing the result of the query.
    """
    return None

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
        num_tokens = len(page.text.split(" "))
        if num_tokens < min_num_tokens:
            result = ""
        else:
            result = "<<article_start>> {} <<article_end>>\n".format(page.text)
    except Exception:
        result = ""
    return result

class WookieepediaScrapingStage(BaseStage):
    """Stage for scraping the data from the wikipedia.
    """
    name = "wokieepedia_scraping"
    logger = logging.getLogger("pipeline").getChild("wookieepedia_scraping_stage")

    def __init__(self, parent=None, article_type="Jedi_Masters_of_the_Jedi_Order",
                 min_num_tokens=500):
        """Initialization for Wikipedia Scraping stage.

        Args:
            parent: The parent stage.
            article_type: type of articles to look for (e.g. person, planet).
            min_num_tokens: The minimum number of tokens in the article.
        """
        super(WikipediaScrapingStage, self).__init__(parent)
        self.article_type = self.article_type
        self.min_num_tokens = min_num_tokens

    def pre_run(self):
        """The function that is executed before the stage is run.
        """
        self.logger.info("=" * 40)
        self.logger.info("Executing wikipedia scraping stage")
        self.logger.info("-" * 40)

    def run(self):
        """Scraps the articles from wikipedia.

        Returns:
            True if the stage execution succeded, False otherwise.
        """
        self.logger.info("Querying wikidata for articles to scrape...")
        article_list = get_article_list(self.search_query_file_path)
        self.logger.info("Got {} articles.".format(len(article_list)))

        site = pywikibot.Site("en", "starwars")
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
