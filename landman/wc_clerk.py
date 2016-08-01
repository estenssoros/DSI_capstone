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
from string import maketrans, punctuation
import os
import json
import numpy as np
import itertools
from sklearn.cross_validation import train_test_split


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

    spaces = ' ' * len(punctuation)
    table = maketrans(punctuation, spaces)
    text = text.translate(table)
    text = re.sub('[^a-z 0-9]+', ' ', text)
    text = ' '.join(text.split())
    return text


def find_ngrams(input_list, n):
    return zip(*[input_list[i:] for i in range(n)])


def gen_ngrams(text):
    text = text.split()
    n_grams = find_ngrams(text, 3)
    return Counter(n_grams)


def make_trie(text):
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
    doc_num, text = arg
    if vocab is None:
        words = []
        d = 'traintext/'
        docs = [d + f for f in os.listdir(d) if f.endswith('.txt')]
        for doc in docs:
            words.append(read_text(doc))
        words = ' '.join(words)
        vocab = list(set(words.split()))
        trie = make_trie(words)

    if maxword is None:
        maxword = max(len(x) for x in vocab)

    word_arr = []
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

    return doc_num, ' '.join(word_arr)


def find_legal_description(arg):
    doc, text = arg
    with open('text/keywords.txt') as f:
        key_words = [line.replace('\n', '') for line in f]
    indexes = []
    for key in key_words:
        indexes.extend([m.start() for m in re.finditer(key, text)])
    try:
        if 'towns' in text:
            text = text[min(indexes):min(indexes) + 150]
        else:
            text = text[min(indexes) - 40:min(indexes) + 150]
    except:
        text = ''
    return doc, text


def find_years(arg):
    doc, text = arg
    with open('years.json') as f:
        years = json.load(f)['years']
    indexes = []
    for year in years:
        indexes.extend([m.start() for m in re.finditer(year + ' year', text)])
    year_text = ' '.join([text[idx:idx + 15] for idx in indexes])
    return doc, year_text


def multi_func(df, func, columns):
    tuples = [tuple(x) for x in df.values]
    pool = Pool(processes=cpu_count() - 1)
    results = pool.map(func, tuples)
    pool.close()
    pool.join()
    return pd.DataFrame(results, columns=columns)


def parse_legal_descr(arg):
    doc, text = arg

    # township
    towns = re.findall('town\w+ [\d ]+|town\\w+ [\d]+', text)
    for t in towns:
        text = text.replace(t, '')
    text = text.strip()
    if text.startswith('north'):
        text = text[5:].strip()

    # section
    secs = [m.start() for m in re.finditer('sect\w+ [\d]+', text)]
    secs.append(len(text))
    qtrs = [text[secs[i]:secs[i + 1]] for i in range(len(secs) - 1)]

    sections = []
    for i, sec in enumerate(qtrs):
        r = re.findall('sect\w+ [\d]+', sec)
        sections.append(''.join(r))
        qtrs[i] = qtrs[i].replace(r[0], '')
        text = text.replace(sec, '')

    # range
    text = ''.join(text.split())
    ranges = re.findall('\d+w', text)

    towns = [re.findall('\d+', ''.join(t.split())) for t in towns]
    sections = [re.findall('\d+', ''.join(s.split())) for s in sections]
    ranges = [re.findall('\d+', ''.join(r.split())) for r in ranges]

    # quarters
    quarters = []
    expressions = ['n\d', 's\d', '[n|s][e|w]\d*']
    for qtr in qtrs:
        quarters.append([''.join(re.findall(exp, qtr)) for exp in expressions])
    print qtrs
    return doc, towns, sections, ranges, quarters


def apply_funcs(df):
    new_df = multi_func(df[['doc', 'text']], find_legal_description, ['doc', 'legal_text'])
    print 'legal description'
    df = pd.merge(df, new_df, how='left', on='doc')

    new_df = multi_func(df[['doc', 'text']], find_years, ['doc', 'years'])
    print 'lease years'
    df = pd.merge(df, new_df, how='left', on='doc')

    new_df = multi_func(df[['doc', 'legal_text']], parse_legal_descr, ['doc', 'town', 'sec', 'range', 'quarters'])
    print 'legal description'
    df = pd.merge(df, new_df, how='left', on='doc')
    return df


if __name__ == '__main__':
    os.system('clear')
    # df = get_text_df('data/text_data_sample.csv')
    # df = multi_func(df, find_words, ['doc', 'text'])
    # df.to_pickle('data/corrected_text.pickle')
        # df = pd.read_pickle(
    # df.to_pickle('data/all_data.pickle')
