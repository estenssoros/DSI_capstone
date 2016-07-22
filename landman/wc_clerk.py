from LM_Text import loop_text
from LM_OCR import loop_ocr
from LM_AWS import sync_read
from os import system

if __name__ == '__main__':
    system('clear')
    print 'Welcome to Virtual Landman!\n'
    print 'At this stage, options are:'
    print '  -loop_text(#loops)'
    print '  -loop_ocr(#loops)'
    print '  -sync_read(r=True)'
