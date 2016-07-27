from LM_Text import loop_text
from LM_OCR import loop_ocr
from LM_AWS import sync_read, write_to_s3, connect_s3, get_docs_from_s3, read_from_s3
from LM_Util import welcome, get_words
from LM_SpellCheck import correct
from os import system
import pandas as pd
import time
from nltk.stem.porter import PorterStemmer
from math import log
from multiprocessing import Pool, cpu_count
import re
import enchant


def clean_text(df):
    df['text'] = df['text'].str.lower().str.replace('[^a-z]', ' ')
    df['text'] = df.apply(lambda x: ' '.join(x['text'].split()), axis=1)
    return df


def find_words(text, words=None, maxword=None, wordcost=None):
    if words is None:
        with open('words_by_frequency.txt') as f:
            words = set(f.read().split())

    if maxword is None:
        maxword = max(len(x) for x in words)

    if wordcost is None:
        wordcost = dict((k, log((i + 1) * log(len(words)))) for i, k in enumerate(words))

    results = []
    while len(text) > 0:
        start = 0
        end = start + 1
        options = []
        for i in range(maxword):
            if text[start:end] in words:
                options.append(text[start:end])
            end += 1
        if options:
            option = min(options, key=lambda x: wordcost.get(x))
            text = text[len(option):]
            results.append(option)
        else:
            text = text[1:]

    return results


def parse_clusters(args, words=None, maxword=None, wordcost=None):
    doc, text = args
    clusters = text.split()
    results = []
    for cluster in clusters:
        if cluster in words:
            results.append(cluster)
        else:
            results.extend(find_words(cluster))
    return results


def multi_find_words(df):
    tuples = [tuple(x) for x in df.values]
    pool = Pool(processes=cpu_count() - 1)
    results = pool.map(find_words, tuples)
    return pd.DataFrame(results, columns=['doc', 'text'])


if __name__ == '__main__':
    system('clear')
    welcome()
    df = pd.read_csv('data/text_data_sample.csv')
    df = clean_text(df)
    # df = multi_find_words(df)
