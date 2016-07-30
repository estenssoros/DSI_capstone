from __future__ import division
from LM.LM_Text import loop_text, clean_docs, multi_doc
from LM.LM_OCR import loop_ocr
from LM.LM_AWS import sync_read, write_to_s3, connect_s3, get_docs_from_s3, read_from_s3
from LM.LM_Util import welcome, get_words
from LM.LM_SpellCheck import correct
from LM.LM_Plot import plot_hist
from os import system
import pandas as pd
import time
from nltk.stem.porter import PorterStemmer
from math import log
from multiprocessing import Pool, cpu_count
import re
from collections import Counter, defaultdict
from Levenshtein import distance
from string import maketrans, punctuation
import os
import json
import matplotlib.pyplot as plt


def get_text_df(fname):
    df = pd.read_csv(fname)
    df['text'] = df['text'].str.lower().str.replace('[^a-z0-9]', ' ')
    df['text'] = df.apply(lambda x: ''.join(x['text'].split()), axis=1)
    print 'Data frame read in!'
    return df


def make_trie(text, window=7):
    start = 0
    end = start + window
    tries = []
    while len(text) > 0:
        tries.append(text[start:end])
        text = text[1:]

    _end = '_end_'
    root = dict()
    for trie in tries:
        current_dict = root
        for letter in trie:
            current_dict = current_dict.setdefault(letter, {})
        current_dict[_end] = _end
    return root


def read_text(fname):
    with open(fname) as f:
        text = f.read()
    text = text.lower()

    spaces = ' ' * len(punctuation)
    table = maketrans(punctuation, spaces)
    text = text.translate(table)

    text = re.sub('[^A-Za-z 0-9]+', ' ', text)
    text = ' '.join(text.split())
    return text


def score_word(word, trie):
    if len(word) == 0:
        return 0
    print word
    letter = word[0]
    if letter in trie:
        return  1 + score_word(word[1:], trie[letter])
    else:
        return 0


if __name__ == '__main__':
    system('clear')
    # welcome()
    # df = get_text_df('data/text_data_sample.csv')
    # text = df.loc[0, 'text']
    # df['char_tries'] = df.apply(lambda x: character_trie(7, x['text']), axis=1)
    # df['b_char_tries'] = df.apply(lambda x: character_trie(7, x['text'][::-1]), axis=1)
    # docs = [f for f in os.listdir('textdocs/') if f.endswith('.txt')]
    # for doc in docs:
    #     correct = read_text('train_text/{}'.format(doc))
    #     bad = read_text('textdocs/{}'.format(doc))
    #     n = len(correct)
    #     E = distance(correct, bad)
    #     print '{0} character accuracy: {1:.2f}%'.format(doc, (n - E) / n)
    # for doc in docs:
    #     text = read_text('train_text/{}'.format(doc))
    #     trie = make_trie(text)
    #     with open('tries/{0}_ls.json'.format(doc.replace('.txt', '')), 'w') as f:
    #         json.dump(trie, f)
    #     trie = make_trie(text[::-1])
    #     with open('tries/{0}_lp.json'.format(doc.replace('.txt', '')), 'w') as f:
    #         json.dump(trie, f)
    with open('tries/DOC100S1082_0_ls.json') as f:
        trie = json.load(f)
