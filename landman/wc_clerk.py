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


def get_text_df(fname):
    df = pd.read_csv(fname)
    df['text'] = df['text'].str.lower().str.replace('[^a-z0-9]', ' ')
    df['text'] = df.apply(lambda x: ''.join(x['text'].split()), axis=1)
    print 'Data frame read in!'
    return df


def condense(lst):
    lst = [''.join(x.split()) for x in lst]
    return ''.join(lst)


def reg_key_words(key_dict):
    results = defaultdict(list)

    for k, v in key_dict.iteritems():
        if k == 'township':
            results[k].append(re.findall('[0-9]+n', v))
        if k == 'section':
            results[k].append(re.findall('[0-9]+', v))
        if k == 'range':
            results[k].append(re.findall('[0-9]+w', v))
    return results


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

    maxword = max(max(len(x) for x in keywords), maxword)

    results = []
    found = []
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
            end += 1

        if options:
            option = max(options, key=lambda x: len(x))
            text = text[len(option):]
            results.append(option)
        else:
            text = text[1:]

    # find index location of key words in main text
    lst = list(results)
    indexes = []
    for word in found:
        idx = lst.index(word)
        indexes.append(idx)
        lst[idx] = ""

    # pull folowing text of key words
    df = pd.DataFrame(columns=['doc', 'township', 'section', 'range'])
    key_dict = {}
    for i in range(len(found) - 1):
        key_lst = results[indexes[i] + 1:indexes[i + 1]]
        if len(key_lst) > 10:
            key_lst = key_lst[:10]
        if found[i] in key_dict:
            key_dict[found[i]].append(''.join(key_lst))
        else:
            key_dict[found[i]] = [''.join(key_lst)]

    # for k, v in key_dict.iteritems():
    #     key_dict[k] = [condense(x) for x in v]

    # options = ['ne\d*', 'nw\d*', 'se\d*', 'sw\d*', 'n\d*', 'n\d*', 'all']
    # quarters = []
    # if 'section' in key_dict:
    #     for i, section in enumerate(key_dict['section']):
    #         section_qtr = []
    #         for option in options:
    #             re_find = re.findall(option, section)
    #             if len(re_find) > 0:
    #                 section_qtr.append(re_find)
    #                 for x in re_find:
    #                     section = key_dict['section'][i].replace(x, '')
    #         key_dict['section'][i] = section
    #
    #         if section_qtr:
    #             quarters.append((i, section_qtr))
    # key_dict = reg_key_words(key_dict)
    # if quarters:

    return doc, ' '.join(results), key_dict


def multi_find_words(df):
    tuples = [tuple(x) for x in df.values]
    pool = Pool(processes=cpu_count() - 1)
    results = pool.map(find_words, tuples)
    return pd.DataFrame(results, columns=['doc', 'text', 'keywords'])


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

def read_key(key):
    doc = key.name.replace('textdocs/', '').replace('.txt', '')
    text = key.get_contents_as_string()
    w_count = len(text.split())
    size = key.size
    return doc, w_count, size

def text_info():
    b = connect_s3()
    keys = [key for key in b.list('textdocs/') if key.endswith('.txt')]
    print 'keys read in!'
    pool = Pool(processes=cpu_count()-1)
    results = pool.map(read_key, keys)
    return pd.DataFrame(results, columns=['doc', 'w_count', 'size'])

if __name__ == '__main__':
    system('clear')
    welcome()
