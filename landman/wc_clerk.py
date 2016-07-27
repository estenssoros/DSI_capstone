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
from collections import Counter, defaultdict


def get_text_df(fname):
    df = pd.read_csv(fname)
    # replace=['\n','\xe2','\x80','\x98','\x94','\x9','\x94']
    # for r in replace:
    #     df['text'] = df['text'].str.replace(r,' ')
    df['text'] = df['text'].str.lower().str.replace('[^a-z]', ' ')
    df['text'] = df.apply(lambda x: ''.join(x['text'].split()), axis=1)
    print 'Data frame read in!'
    return df


def find_words(args, words=None, maxword=None, wordcost=None):
    '''

    '''
    doc, text = args

    if words is None:
        with open('words_by_frequency.txt') as f:
            words = set(f.read().lower().split())

    if maxword is None:
        maxword = max(len(x) for x in words)

    if wordcost is None:
        # wordcost = dict((k, log((i + 1) * log(len(words)))) for i, k in enumerate(words))
        wordcost = defaultdict(lambda: 1)
        for word in words:
            wordcost[word] += 1

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
            option = max(options, key=lambda x: len(x))
            text = text[len(option):]
            results.append(option)
        else:
            text = text[1:]

    return doc, ' '.join(results)


def multi_find_words(df):
    tuples = [tuple(x) for x in df.values]
    pool = Pool(processes=cpu_count() - 1)
    results = pool.map(find_words, tuples)
    return pd.DataFrame(results, columns=['doc', 'text'])


def find_ngrams(input_list, n):
    return zip(*[input_list[i:] for i in range(n)])


def generate_n_grams():
    with open('words_by_frequency.txt') as f:
        text = f.read().split()
    n_grams = []
    for i in range(2, 6):
        print i
        n_grams.extend(find_ngrams(text, i))
    return Counter(n_grams)


def replace_word(word, repl):
    with open('words_by_frequency.txt') as f:
        text = f.read()

    text = text.replace(word, repl)

    with open('words_by_frequency.txt', 'w') as f:
        f.write(text)

    print 'done!'

if __name__ == '__main__':
    system('clear')
    welcome()
    df = get_text_df('data/text_data_sample.csv')
    # df = multi_find_words(df)
    # n_grams = generate_n_grams()
