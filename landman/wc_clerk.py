from __future__ import division
from LM.LM_Text import loop_text, clean_docs, multi_doc
from LM.LM_OCR import loop_ocr
from LM.LM_AWS import sync_read, write_to_s3, connect_s3, get_docs_from_s3, read_from_s3
from LM.LM_Util import welcome, get_words
import pandas as pd
import time
from multiprocessing import Pool, cpu_count
import re
from collections import Counter, defaultdict
from Levenshtein import distance
from string import maketrans, punctuation
import os
import json
import numpy as np
import itertools
from fuzzywuzzy import process


def get_text_df(fname):
    df = pd.read_csv(fname)
    df['text'] = df['text'].str.lower().str.replace('[^a-z0-9]', ' ')
    df['text'] = df.apply(lambda x: ''.join(x['text'].split()), axis=1)
    print 'Data frame read in!'
    return df


def read_text(fname):
    with open(fname) as f:
        text = f.read()
    text = text.lower()

    # spaces = '.' * len(punctuation)
    # table = maketrans(punctuation, spaces)
    # text = text.translate(table)
    punc = set('.,?!')
    for p in punc:
        text = text.replace(p, '')
    punc = set([punctuation])
    for p in punc:
        text = text.replace(p, ' ')
    text = re.sub('[^a-z 0-9]+', ' ', text)
    text = ' '.join(text.split())
    return text


def find_ngrams(input_list, n):
    return zip(*[input_list[i:] for i in range(n)])


def gen_ngrams(text):
    text = text.split()
    # n_grams = []
    # for i in range(2, 6):
    # n_grams.extend(find_ngrams(text, i))
    n_grams = find_ngrams(text, 3)
    return Counter(n_grams)


def make_trie(text):
    # tries = text_windows(text, window)
    tries = text.split()
    _end = '_end_'
    root = dict()
    for trie in tries:
        current_dict = root
        for letter in trie:
            current_dict = current_dict.setdefault(letter, {})
        current_dict[_end] = _end
    return root


def weight_node(trie):
    weight = 0
    for k, v in trie.iteritems():
        if k == '_end_':
            return 1
        else:
            weight += weight_node(trie[k])
    return weight


def entropy(trie):
    n_weight = weight_node(trie)
    return sum([weight_node(n) / n_weight * np.log(weight_node(n)) / n_weight for c, n in trie.iteritems()])


def word_entropy(segment, trie):
    if len(segment) == 0:
        return entropy(trie)

    letter = segment[0]
    if letter in trie:
        return word_entropy(segment[1:], trie[letter])
    else:
        print segment
        return entropy(trie)


def word_lsv(segment, trie):
    if len(segment) == 0:
        return len(trie)
    letter = segment[0]
    if letter in trie:
        return word_lsv(segment[1:], trie[letter])
    else:
        return len(trie)


def find_words(arg, vocab=None, maxword=None):  # , keywords=None):
    doc, text = arg
    if vocab is None:
        words = []
        d = 'train_text/'
        docs = [d + f for f in os.listdir(d) if f.endswith('.txt')]
        for doc in docs:
            words.append(read_text(doc))
        words = ' '.join(words)
        vocab = list(set(words.split()))
        trie = make_trie(words)

    if maxword is None:
        maxword = max(len(x) for x in vocab)

    word_arr, found, missed = [], [], []
    while len(text) > 0:
        start = 0
        end = start + 1
        options = []
        for i in range(maxword):
            test_word = text[start:end]
            if test_word in vocab:
                options.append(test_word)
            end += 1

        if options:
            option = max(options, key=lambda x: len(x))
            text = text[len(option):]
            word_arr.append(option)
        else:
            text = text[1:]

    return doc, ' '.join(word_arr)


def multi_find_words(df):
    tuples = [tuple(x) for x in df.values]
    pool = Pool(processes=cpu_count() - 1)
    results = pool.map(find_words, tuples)
    pool.close()
    pool.join()
    return pd.DataFrame(results, columns=['doc', 'text'])

if __name__ == '__main__':
    os.system('clear')
    df = get_text_df('data/text_data_sample.csv')
    df = multi_find_words(df)
