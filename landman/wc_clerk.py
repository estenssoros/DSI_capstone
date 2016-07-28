from LM.LM_Text import loop_text
from LM.LM_OCR import loop_ocr
from LM.LM_AWS import sync_read, write_to_s3, connect_s3, get_docs_from_s3, read_from_s3
from LM.LM_Util import welcome, get_words
from LM.LM_SpellCheck import correct
from os import system
import pandas as pd
import time
from nltk.stem.porter import PorterStemmer
from math import log
from multiprocessing import Pool, cpu_count
import re
from collections import Counter, defaultdict
from textblob import TextBlob


def get_text_df(fname):
    df = pd.read_csv(fname)
    df['text'] = df['text'].str.lower().str.replace('[^a-z0-9]', ' ')
    df['text'] = df.apply(lambda x: ''.join(x['text'].split()), axis=1)
    print 'Data frame read in!'
    return df


def find_words(args, words=None, maxword=None, keywords=None):
    '''

    '''
    doc, text = args

    if words is None:
        with open('text/words_by_frequency.txt') as f:
            words = set(f.read().lower().split())

    if maxword is None:
        maxword = max(len(x) for x in words)

    if keywords is None:
        with open('text/keywords.txt') as f:
            keywords = set(f.read().lower().split())
            min_key = min(len(x) for x in keywords)

    maxword = max(max(len(x) for x in keywords), maxword)

    results = []
    found=[]
    while len(text) > 0:
        start = 0
        end = start + 1
        options = []
        for i in range(maxword):
            test_word = text[start:end]
            if test_word in words:
                options.append(test_word)
                if test_word in keywords:
                    found.append(test_word)
            # elif len(test_word) > 3 and key_word_loc == None and test_word[0] in [w[0] for w in keywords]:
            #     corr = correct(test_word)
            #     print test_word, corr
            #     if corr in keywords:
            #         key_word_loc = start
            end += 1

        if options:
            option = max(options, key=lambda x: len(x))
            text = text[len(option):]
            results.append(option)
        else:
            text = text[1:]

    return doc, ' '.join(results), found


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
