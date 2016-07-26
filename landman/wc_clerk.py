from LM_Text import loop_text
from LM_OCR import loop_ocr
from LM_AWS import sync_read, write_to_s3, connect_s3, get_docs_from_s3
from LM_Util import welcome, get_words
from os import system
import pandas as pd
import time
from nltk.stem.porter import PorterStemmer
from math import log


def clean_text(df):
    df['text'] = df['text'].str.lower().str.replace('[^a-z]', ' ')
    df['text'] = df.apply(lambda x: ''.join(x['text'].split()), axis=1)

    # words, max_length = get_words()

    return df


def slider(text, vocab, max_length, start=0):
    words = []
    end = start + 1
    while True:
        test_word = text[start:end]
        print start, end, test_word
        time.sleep(0.1)
        if test_word in vocab:
            words.append(test_word)
            print 'found word {0}!'.format(test_word)
            start = end
            end = start + 1
        else:
            end += 1
        if len(test_word) > max_length:
            start += 1
            end = start + 1
        if end == len(text):
            break

    return words


def infer_spaces(s):
    words = open("words_by_frequency.txt").read().split()
    wordcost = dict((k, log((i + 1) * log(len(words)))) for i, k in enumerate(words))
    maxword = max(len(x) for x in words)
    """Uses dynamic programming to infer the location of spaces in a string
    without spaces."""

    # Find the best match for the i first characters, assuming cost has
    # been built for the i-1 first characters.
    # Returns a pair (match_cost, match_length).
    def best_match(i):
        candidates = enumerate(reversed(cost[max(0, i - maxword):i]))
        return min((c + wordcost.get(s[i - k - 1:i], 9e999), k + 1) for k, c in candidates)

    # Build the cost array.
    cost = [0]
    for i in range(1, len(s) + 1):
        c, k = best_match(i)
        cost.append(c)

    # Backtrack to recover the minimal-cost string.
    out = []
    i = len(s)
    while i > 0:
        c, k = best_match(i)
        assert c == cost[i]
        out.append(s[i - k:i])
        i -= k

    return " ".join(reversed(out))

if __name__ == '__main__':
    system('clear')
    # welcome()
    df = pd.read_csv('data/text_data_sample.csv')
    df = clean_text(df)
    text = df.loc[0, 'text']
    words, max_length = get_words()
    # slider(text, words, max_length)
    print text
    print infer_spaces(text)
