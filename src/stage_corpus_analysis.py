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
import nltk
from nltk.corpus import stopwords
from scipy import stats
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import seaborn as sns

from collections import deque
from statistics import mean
from statistics import pstdev
from copy import copy

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
        self.corpus_stopwords = ['wa']

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

        output_file_path = join(constants.OUTPUT_PATH,
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
        df_corpus = df[['text_strp_punct']].copy()

        try:
            analysis_type = '1_cleaning_summary.csv'
            filename = str(output_file_path[:-3] + analysis_type)
            df.to_csv(filename, index=False)

        except:
            self.logger.info("Created Summary File 1 but did not write to disk")

        df['text_strp_punct'].replace('', np.nan, inplace=True)

        df = df[['text_strp_punct']].dropna()

        df = df[~df['text_strp_punct'].str.contains("<<")]

        df['text_strp_punct'] = df['text_strp_punct'].astype('string')


        self.logger.info('Stripped Punctuation but not <<article_start>> or other tags')

        df.rename(columns={'text_strp_punct': 'text'}, inplace=True)
        df.reset_index(drop=True, inplace=True)

        summary_2 = df.stb.freq(['text'])
        try:
            analysis_type = '2_raw_text_summary.csv'
            filename = str(output_file_path[:-3] + analysis_type)
            summary_2.to_csv(filename, index=False)
        except:
            self.logger.info("Created Text Summary 2 File but did not write to disk")

        stop_words = list(stopwords.words('english'))
        stop_words.extend(self.corpus_stopwords)

        df['stop_flag'] = df[['text']].isin(stop_words).any(1)

        stops = df.copy()

        stops = stops[stops['stop_flag'] == True].reset_index(drop=True)
        stops.drop(columns='stop_flag', inplace=True)
        summary_3 = stops.stb.freq(['text'])

        self.logger.info('Flagged and Removed Stop Words')

        try:
            analysis_type = '3_stop_text_summary.csv'
            filename = str(output_file_path[:-3] + analysis_type)
            summary_3.to_csv(filename, index=False)
        except:
            self.logger.info("Created Summary 3 File but did not write to disk")

        summary_4 = df[df['stop_flag'] == False].reset_index(drop=True).drop(columns='stop_flag')
        summary_4 = summary_4.stb.freq(['text'])
        try:
            analysis_type = '4_cleaned_text_summary.csv'
            filename = str(output_file_path[:-3] + analysis_type)
            summary_4.to_csv(filename, index=False)
        except:
            self.logger.info("Created Summary 4 File but did not write to disk")

        try:
            analysis_type = '5_cleaned_text.csv'
            filename = str(output_file_path[:-3] + analysis_type)
            df.to_csv(filename, index=False)

            analysis_type = '5_cleaned_text.pickle'
            filename = str(output_file_path[:-3] + analysis_type)
            df.to_pickle(filename, protocol=2)
        except:
            self.logger.info("Created Summary Text Analysis File but did not write to disk")


        stats_dict = {}

        unique_words = len(df['text'].unique())
        stats_dict.update({"unique_words_bf_stop":unique_words})

        df = df[df['stop_flag'] == False].reset_index(drop=True).drop(columns='stop_flag')
        unique_words = len(df['text'].unique())
        stats_dict.update({"unique_words_af_stop": unique_words})

        frequency = df.stb.freq(['text'])

        try:
            analysis_type = '6_text_frequency_t10.csv'
            filename = str(output_file_path[:-3] + analysis_type)
            frequency[:10].to_csv(filename, index=False)

        except:
            self.logger.info("Created Summary Text Analysis File but did not write to disk")

        df = df.groupby(['text'])['text'].agg('count').reset_index(name='count')

        df['text_length'] = df['text'].str.len()

        df = df[df['text_length'] < 30].reset_index(drop=True)

        unique_words = len(df['text'].unique())
        stats_dict.update({"unique_words_af_freq": unique_words})

        df['text_length_z_score'] = np.abs(stats.zscore(df['text_length']))

        word_cloud_words = ' '.join(df['text'].values)

        wordcloud = WordCloud(width=800, height=800,
                              background_color='white',
                              min_font_size=10).generate(word_cloud_words)

        plt.figure(figsize=(8, 8), facecolor=None)
        plt.imshow(wordcloud)
        plt.axis("off")
        plt.tight_layout(pad=0)
        plt.title('A Word Cloud of Subject Matter Universal Corpus')

        analysis_type = 'word_cloud.png'
        filename = str(output_file_path[:-3] + analysis_type)
        plt.savefig(filename)

        plt.figure()
        df['z_score_bin'] = pd.qcut(df['text_length_z_score'], 3).reset_index(drop=True)
        ax = sns.displot(data=df
                         , x='text_length'
                         , y='count'
                         , hue='z_score_bin'
                         )

        plt.ylabel('Word Count')
        plt.xlabel('Word Character Length')
        plt.title('Frequency Distribution of Corpus\n Organized by Corpus Word Count and Word Text Length')
        plt.tight_layout()
        analysis_type = 'word_frequency.png'
        filename = str(output_file_path[:-3] + analysis_type)
        plt.savefig(filename)

        corpus_text = deque(df_corpus['text_strp_punct'].values)

        corpus_dict = {}
        counter = 0
        accu = []

        while corpus_text:
            start_marker = ['<<article_start>>']
            curr = corpus_text.popleft()

            if any(m in curr for m in start_marker):
                if accu:
                    counter += 1
                    corpus_dict.update({counter: (accu)})
                accu = []
                accu.append(curr)
                curr = corpus_text.popleft()

            if accu:
                accu.append(curr)

        stats_dict.update({"total_articles": len(corpus_dict)})

        article_lengths = [len(value) for key, value in corpus_dict.items()]

        stats_dict.update({"article_max_len": max(article_lengths)})
        stats_dict.update({"article_min_len": min(article_lengths)})
        stats_dict.update({"article_mean_len": mean(article_lengths)})
        stats_dict.update({"article_stdv_len": pstdev(article_lengths)})

        self.logger.info(stats_dict)

        # uncomment below to make coprus into sparse numpy arr
        # arr = df.to_numpy()


        return True
