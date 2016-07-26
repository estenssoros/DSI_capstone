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
    df['text'] = df.apply(lambda x: ''.join(x['text'].split()), axis=1)
    return df


def find_words(args, words=None, maxword=None, wordcost=None):
    doc, instring = args
    if words is None:
        with open('words_by_frequency.txt') as f:
            words = set(f.read().split())

    if maxword is None:
        maxword = max(len(x) for x in words)

    if wordcost is None:
        wordcost = dict((k, log((i + 1) * log(len(words)))) for i, k in enumerate(words))
    results = []
    while len(instring) > 0:
        start = 0
        end = start + 1
        options = []
        for i in range(maxword):
            if instring[start:end] in words:
                options.append(instring[start:end])
            end += 1
        if options:
            option = min(options, key=lambda x: wordcost.get(x))
            instring = instring[len(option):]
            results.append(option)
        else:
            instring = instring[1:]

    return doc, ' '.join(results)


def multi_find_words(df):
    tuples = [tuple(x) for x in df.values]
    pool = Pool(processes=cpu_count() - 1)
    results = pool.map(find_words, tuples)
    return pd.DataFrame(results, columns=['doc', 'text'])

DICT = enchant.Dict("en_US")

def enchant_text(word):
    if DICT.check(word):
        return word
    else:
        try:
            return DICT.suggest(word)[0]
        except:
            return " "

def clean_text_file():
    with open('clean.txt') as f:
        text = f.read()
    text = text.lower()
    text = re.findall('[a-z]+', text)
    text = [x for x in text if len(x) > 2]

    text = [enchant_text(x) for x in text]
    with open('clean.txt', 'w') as f:
        f.write(' '.join(text).lower())

def update_vocab():
    fname = 'words_by_frequency.txt'
    clean = 'clean.txt'
    with open(clean) as f:
        text = f.read()
    with open(clean, 'w') as f:
        f.write('')

    with open(fname) as f:
        master = f.read()

    master = master + ' ' + text

    with open(fname,'w') as f:
        f.write(master)
    print 'written to {0}'.format(fname)


if __name__ == '__main__':
    system('clear')
    # welcome()
    # df = pd.read_csv('data/text_data_sample.csv')
    # df = clean_text(df)
    # df = multi_find_words(df)
