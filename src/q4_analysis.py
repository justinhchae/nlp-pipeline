## Testing some stuff here

import pandas as pd
import numpy as np
import string
import matplotlib.animation as animation
import matplotlib.pyplot as plt
import seaborn as sns
import sidetable
import nltk
from nltk.corpus import stopwords
from scipy import stats
from wordcloud import WordCloud

from collections import deque
from statistics import mean
from statistics import pstdev

df = pd.read_pickle('../analysis/text_analysis.pickle')

unique_words = len(df['text'].unique())
# print(unique_words)


df = df[df['stop_flag']==False].reset_index(drop=True).drop(columns='stop_flag')

unique_words = len(df['text'].unique())
# print(unique_words)

frequency = df.stb.freq(['text'])

frequency[:10].to_csv('../analysis/top_ten.csv')

df = df.groupby(['text'])['text'].agg('count').reset_index(name='count')

df['text_length'] = df['text'].str.len()

df = df[df['text_length'] < 15].reset_index(drop=True)

unique_words = len(df['text'].unique())
# print(unique_words)

df['text_length_z_score'] = np.abs(stats.zscore(df['text_length']))

word_cloud_words = ' '.join(df['text'].values)
# print(word_cloud_words)

wordcloud = WordCloud(width = 800, height = 800,
                background_color ='white',
                min_font_size = 10).generate(word_cloud_words)

plt.figure(figsize=(8, 8), facecolor=None)
plt.imshow(wordcloud)
plt.axis("off")
plt.tight_layout(pad=0)
plt.title('A Word Cloud of Subject Matter Universal Corpus')
plt.savefig('../analysis/word_cloud.png')
# plt.show()

plt.figure()

counts = df.sort_values(by=['count'], ascending=False).reset_index(drop=True)

print('after count', counts['count'].sum())

counts = counts[:5]

df['z_score_bin'] = pd.qcut(df['text_length_z_score'], 3).reset_index(drop=True)

ax = sns.displot(data=df
            ,x='text_length'
            ,y='count'
            ,hue='z_score_bin'
            )

top = counts[['text', 'count']].values

plt.ylabel('Word Count')
plt.xlabel('Word Character Length')
plt.title('Frequency Distribution of Corpus\n Organized by Corpus Word Count and Word Text Length')
plt.tight_layout()
plt.savefig('../analysis/q4_fig1.png')
# plt.show()

df = pd.read_csv('../analysis/text_processing.csv')

df = df.dropna()

corpus_text = deque(df['text_strp_punct'].values)

# def corpus_splitter(x):
#     curr = x[0]
#     rest = x[1:]
#     markers = ['<<', '>>']
#     if any(m in curr for m in markers):
#         print(curr)
#
#     return corpus_splitter(rest)
#
# corpus_splitter(corpus_text)

corpus_dict = {}
counter = 0
# ['<<article_start>>', 'something', 'something', '<<article_end>>']
accu = []

while corpus_text:
    start_marker = ['<<article_start>>']
    curr = corpus_text.popleft()


    if any(m in curr for m in start_marker):
        if accu:
            counter += 1
            # print(counter, accu)
            corpus_dict.update({counter:(accu)})
        accu = []
        accu.append(curr)
        curr = corpus_text.popleft()

    if accu:
        accu.append(curr)


print('Total Articles', len(corpus_dict))

article_lengths = [len(value) for key, value in corpus_dict.items()]

print('Max Article Length',max(article_lengths))
print('Min Article Length',min(article_lengths))
print('Average Article Length',mean(article_lengths))
print('Standard Deviation in Article Length',pstdev(article_lengths))

# print(corpus_dict)

df = pd.DataFrame.from_dict(corpus_dict, orient='index')
df = df.fillna(np.nan)

print(df)

arr = df.to_numpy()


