from LM_Text import loop_text
from LM_OCR import loop_ocr
from LM_AWS import sync_read, write_to_s3, connect_s3
from LM_Util import welcome
from os import system
import pandas as pd
from multiprocessing import cpu_count, Pool
from string import maketrans, punctuation


def word_count_docs(key):
    text = key.get_contents_as_string()
    text = text.replace('\n', ' ')
    w_count = len(text.split())
    doc = key.name.replace('textdocs/', '').replace('.txt', '')
    size = key.size
    return doc, w_count, size


def clean_docs(key):
    text = key.get_contents_as_string()
    spaces = ' ' * len(punctuation)
    table = maketrans(punctuation, spaces)
    text = text.translate(table)
    replace = ['\n','\x0c']
    for r in replace:
        text = text.replace(r, ' ')
    doc = key.name.replace('textdocs/', '').replace('.txt', '')
    return doc, text


def multi_word_count(func, cols):
    b = connect_s3()
    keys = [key for key in list(b.list('textdocs/')) if key.name.endswith('.txt')]
    print 'keys read in!'
    print 'beggining document analysis!'
    pool = Pool(processes=cpu_count() - 1)
    results = pool.map(func, keys)
    print 'done!'
    return pd.DataFrame(results, columns=cols)


if __name__ == '__main__':
    system('clear')
    welcome()
