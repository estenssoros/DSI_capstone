from LM_Text import loop_text
from LM_OCR import loop_ocr
from LM_AWS import sync_read, write_to_s3, connect_s3
from os import system
import pandas as pd


def word_count_docs():
    df = pd.DataFrame(columns=['doc', 'w_count'])
    b = connect_s3
    for i, key in enumerate(b.list('textdocs/')):
        if key.name.endswith('.txt'):
            text = key.get_contents_as_string()
            text = text.replace('\n', ' ')
            doc = key.name.replace('txtdocs/', '').replace('.txt', '')
            df = df.append({'doc': doc, 'w_count': len(text.split())}, ignore_index=True)
    if i % 1000 == 0:
        print i
    return df


if __name__ == '__main__':
    system('clear')
    print 'Welcome to Virtual Landman!\n'
    print 'At this stage, options are:'
    print '  -loop_text(#loops)'
    print '  -loop_ocr(#loops)'
    print '  -sync_read(r=True)'
    print '  -write_to_s3(file)'
