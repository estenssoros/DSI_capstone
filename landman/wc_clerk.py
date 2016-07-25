from LM_Text import loop_text
from LM_OCR import loop_ocr
from LM_AWS import sync_read, write_to_s3, connect_s3
from os import system
import pandas as pd
from multiprocessing import cpu_count, Pool


def word_count_docs(key):
    text = key.get_contents_as_string()
    text = text.replace('\n', ' ')
    w_count = len(text.split())
    doc = key.name.replace('textdocs/', '').replace('.txt', '')
    size = key.size
    return doc, w_count, size


def multi_word_count():
    b = connect_s3()
    keys = [key for key in list(b.list('textdocs/')) if key.name.endswith('.txt')]
    print 'keys read in!'

    print 'beggining document analysis!'
    pool = Pool(processes=cpu_count() - 1)
    results = pool.map(word_count_docs, keys)
    print 'done!'
    return pd.DataFrame(results, columns=['doc', 'w_count', 'size'])


if __name__ == '__main__':
    system('clear')
    print 'Welcome to Virtual Landman!\n'
    print 'At this stage, options are:'
    print '  -loop_text(#loops)'
    print '  -loop_ocr(#loops)'
    print '  -sync_read(r=True)'
    print '  -write_to_s3(file)'
    print '  -connect_s3()'
