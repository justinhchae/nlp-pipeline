"""Stage for analysing a corpus.
"""
from base_stage import BaseStage
from configuration import run_configuration

import constants

from os.path import join

import logging

import pandas as pd
import numpy as np
import string
from nltk.corpus import stopwords
import sidetable

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

        self.logger.info("Corpus contains {} tokens".format(len(tokens)))
        self.logger.info("Corpus contains {} unique tokens".format(len(set(tokens))))

        df = pd.DataFrame(tokens, columns=['text_orig'])
        
        def remove_unicode(x):
            x = x.encode("ascii", "ignore")
            x = x.decode()
            return x

        df['text_strp_unicode'] = df['text_orig'].apply(lambda x: remove_unicode(x))
        self.logger.info('Stripped Unicode Characters')

        def text_strip(x):
            markers = ['<<', '>>']
            if any(i in x for i in markers):
                return x
            else:
                return x.translate(str.maketrans('', '', string.punctuation))

        df['text_strp_punct'] = df['text_strp_unicode'].apply(lambda x: text_strip(x))

        try:
            df.to_csv('analysis/text_processing.csv', index=False)
        except:
            self.logger.info("Created Text Processing File but did not write to disk")
            self.logger.info("Create folder /analysis and run again")

        df['text_strp_punct'].replace('', np.nan, inplace=True)

        df = df[['text_strp_punct']].dropna()

        df = df[~df['text_strp_punct'].str.contains("<<")]

        df['text_strp_punct'] = df['text_strp_punct'].astype('string')

        self.logger.info('Stripped Punctuation but not <<article_start>> or other tags')

        df.rename(columns={'text_strp_punct': 'text'}, inplace=True)
        df.reset_index(drop=True, inplace=True)

        summary_1 = df.stb.freq(['text'])
        try:
            summary_1.to_csv('analysis/1_raw_summary.csv', index=False)
        except:
            self.logger.info("Created Text Summary 1 File but did not write to disk")
            self.logger.info("Create folder /analysis and run again")

        df['stop_flag'] = df[['text']].isin(stopwords.words('english')).any(1)

        stops = df.copy()

        stops = stops[stops['stop_flag'] == True].reset_index(drop=True)
        stops.drop(columns='stop_flag', inplace=True)
        summary_2 = stops.stb.freq(['text'])

        self.logger.info('Flagged and Removed Stop Words')

        try:
            summary_2.to_csv('analysis/2_stops_summary.csv', index=False)
        except:
            self.logger.info("Created Summary 2 File but did not write to disk")
            self.logger.info("Create folder /analysis and run again")

        summary_3 = df[df['stop_flag'] == False].reset_index(drop=True).drop(columns='stop_flag')
        summary_3 = summary_3.stb.freq(['text'])
        try:
            summary_3.to_csv('analysis/3_text_summary.csv', index=False)
        except:
            self.logger.info("Created Summary 3 File but did not write to disk")
            self.logger.info("Create folder /analysis and run again")

        try:
            df.to_csv('analysis/text_analysis.csv', index=False)
            df.to_pickle('analysis/text_analysis.pickle', protocol=2)
        except:
            self.logger.info("Created Summary Text Analysis File but did not write to disk")
            self.logger.info("Create folder /analysis and run again")

        return True
