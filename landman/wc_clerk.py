from __future__ import division
from LM.LM_AWS import sync_read, write_to_s3, connect_s3, get_docs_from_s3, read_from_s3
from LM.LM_Util import welcome
import pandas as pd
import time
from multiprocessing import Pool, cpu_count
import re
from string import maketrans, punctuation
import os
import json
import numpy as np
from lsseg import text_to_tries, Segmenter, seg_to_HTML, format_line, text_to_HTML


def get_text_df(fname):
    df = pd.read_csv(fname)
    df['text'] = df['text'].str.lower().str.replace('[^a-z0-9]', ' ')
    df['text'] = df.apply(lambda x: ''.join(x['text'].split()), axis=1)
    print 'Data frame read in!'
    return df


def read_text(fname, doc=False):

    with open(fname) as f:
        text = f.read()
    text = text.lower()

    spaces = ' ' * len(punctuation)
    table = maketrans(punctuation, spaces)
    text = text.translate(table)
    text = re.sub('[^a-z 0-9]+', ' ', text)
    text = ' '.join(text.split())
    ind = text.find('oil')
    text = text[ind:]

    if doc:
        base = os.path.basename(fname)
        doc = os.path.splitext(base)[0]
        start = '---{0}---'.format(doc)
        end = '---{0}---'.format(doc)
        return start + text + end
    return text


def find_words(arg, vocab=None, maxword=None):  # , keywords=None):
    doc_num, text = arg
    if vocab is None:
        d = 'traintext/'
        docs = [d + f for f in os.listdir(d) if f.endswith('.txt')]
        words = ' '.join([read_text(doc) for doc in docs])
        vocab = list(set(words.split()))

    if maxword is None:
        maxword = max(len(x) for x in vocab)

    word_arr, missing = [], []
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
    towns = re.findall('town\w+ [\d ]+|town\w+ [\d]+', text)
    for t in towns:
        text = text.replace(t, '')

    # range
    range_text = ''.join(text.split())
    ranges = re.findall('ran\w+ [\d ]+|ran\w+ [\d]+', range_text)
    for r in ranges:
        text = text.replace(r, '')

    # section
    secs = re.findall('sec\w+ [\d ]+|sec\w+ [\d]+', text)
    for s in secs:
        text = text.replace(s, '')

    towns = [re.findall('\d+', ''.join(t.split())) for t in towns]
    ranges = [re.findall('\d+', ''.join(r.split())) for r in ranges]
    secs = [re.findall('\d+', ''.join(s.split())) for s in secs]

    return doc, towns, ranges, secs


def apply_funcs(df):

    print 'find legal description'
    new_df = multi_func(df[['doc', 'clean_text']], find_legal_description, ['doc', 'legal_text'])
    df = pd.merge(df, new_df, how='left', on='doc')

    print 'lease years'
    new_df = multi_func(df[['doc', 'clean_text']], find_years, ['doc', 'years'])
    df = pd.merge(df, new_df, how='left', on='doc')

    print 'parse legal description'
    new_df = multi_func(df[['doc', 'legal_text']], parse_legal_descr, ['doc', 'town', 'range', 'section'])
    df = pd.merge(df, new_df, how='left', on='doc')

    return df


def segment_text(doc_str):
    d = 'textdocs/'
    docs = [d + f for f in os.listdir(d) if f.endswith('.txt')]
    text = ' '.join([read_text(d, doc=True) for d in docs])

    d = 'traintext/'
    docs = [d + f for f in os.listdir(d) if f.endswith('.txt')]
    train_text = ' '.join([read_text(d) for d in docs])
    train_text = '---train---' + train_text + '---train---'

    char_window = 4
    lower_case = True
    peak_type = 'freedom'
    threshold = 0.8

    f_trie, b_trie = text_to_tries(train_text, lowercase=True)
    seg = Segmenter(f_trie, b_trie, window=char_window, lowercase=lower_case, peak=peak_type)

    mer = ' '.join(seg.segment(text, threshold=threshold))
    mer = ' '.join(mer.split())
    indx = [m.start() for m in re.finditer(doc_str, mer)]
    with open('results_{0}_{1}_{2}.txt'.format(char_window, peak_type[0].upper(), threshold), 'w') as f:
        f.write(mer[indx[0] + len(doc_str):indx[1]])

    print 'done!'
if __name__ == '__main__':
    os.system('clear')
    df = get_text_df('data/text_data.csv')
    df = multi_func(df, find_words, ['doc', 'clean_text'])
    df.to_pickle('data/corrected_text.pickle')

    df = pd.read_pickle('data/corrected_text.pickle')
    df = apply_funcs(df)
    df.to_pickle('data/all_data.pickle')

    # fname = 'textdocs/DOC100S825_0.txt'
    # text = read_text(fname)
    # text = ''.join(text.split())
    # word_arr = find_words(text)
