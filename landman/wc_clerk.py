from LM_Text import loop_text
from LM_OCR import loop_ocr
from LM_AWS import sync_read, write_to_s3, connect_s3
from os import system

def check_docs():
    needs_ocr = []
    b = connect_s3
    for fname in b.list('textdocs/'):
        if fname.endswith('.txt'):
            key = b.new_key(fname)
            # text = 

if __name__ == '__main__':
    system('clear')
    print 'Welcome to Virtual Landman!\n'
    print 'At this stage, options are:'
    print '  -loop_text(#loops)'
    print '  -loop_ocr(#loops)'
    print '  -sync_read(r=True)'
    print '  -write_to_s3(file)'
