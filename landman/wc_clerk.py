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
        text = text.replace(p,'')
    punc = set([punctuation])
    for p in punc:
        text = text.replace(p,' ')
    text = re.sub('[^a-z 0-9]+', ' ', text)
    text = ' '.join(text.split())
    return text


def find_ngrams(input_list, n):
    return zip(*[input_list[i:] for i in range(n)])


def gen_ngrams(text):
    text = text.split()
    n_grams = []
    for i in range(2, 6):
        n_grams.extend(find_ngrams(text, i))
    return Counter(n_grams)

def text_windows(text, window):
    start = 0
    end = start + window
    tries = []
    while end <= len(text):
        tries.append(text[start:end])
        start += 1
        end += 1
    return tries


def make_trie(text, window=7):
    tries = text_windows(text, window)
    # tries = text.split()
    _end = '_end_'
    root = dict()
    for trie in tries:
        current_dict = root
        for letter in trie:
            current_dict = current_dict.setdefault(letter, {})
        current_dict[_end] = _end
    return root

def in_trie(segment, trie):
    if len(segment) == 0:
        return True
    letter = segment[0]
    if letter in trie:
        return in_trie(segment[1:], trie[letter])
    else:
        return False
        
def find_words(args, vocab=None, maxword=None):  # , keywords=None):
    '''
    '''
    doc, text = args

    if vocab is None:
        words = []
        d = 'train_text/'
        docs = [d + f for f in os.listdir(d) if f.endswith('.txt')]
        for doc in docs:
            vocab.append(read_text(doc))
        vocab = ' '.join(vocab)
        vocab = set(vocab.split())
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
                if len(missed)>0:
                    word_arr.append(''.join(missed))
                    missed = []
                options.append(test_word)
            end += 1

        if options:
            option = max(options, key=lambda x: len(x))
            text = text[len(option):]
            word_arr.append(options)
        else:
            missed.append(text[0])
            text = text[1:]


    return word_arr
if __name__ == '__main__':
    os.system('clear')
    # welcome()
    df = get_text_df('data/text_data_sample.csv')
    text = df.loc[0, 'text']
    doc = df.loc[0, 'doc']
    # df['char_tries'] = df.apply(lambda x: character_trie(7, x['text']), axis=1)
    # df['b_char_tries'] = df.apply(lambda x: character_trie(7, x['text'][::-1]), axis=1)
    d = 'train_text/'
    docs = [d + f for f in os.listdir(d) if f.endswith('.txt')]
    all_grams = Counter()
    # for doc in docs:
    #     text = read_text(doc)
    #     all_grams.update(gen_ngrams(text))
    word_arr = find_words((doc, text))
