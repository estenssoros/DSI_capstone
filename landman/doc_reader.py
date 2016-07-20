#!/Users/sebastianestenssoro/anaconda/bin/python
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfdevice import PDFDevice
from pdfminer.pdfpage import PDFPage
from pdfminer.converter import TextConverter
from pdfminer.cmapdb import CMapDB
from pdfminer.layout import LAParams
from pypdfocr.pypdfocr import ocr_main
from multiprocessing import Pool, cpu_count
import os
import time
# main


def convert_pdfs(from_dir, to_dir, char_margin=2.0, line_margin=0.5, word_margin=0.1):
    # debug option
    debug = 0
    # input option
    password = ''
    pagenos = set()
    maxpages = 0
    outtype = "text"
    imagewriter = None
    rotation = 0
    layoutmode = 'normal'
    codec = 'utf-8'
    # pageno = 1
    scale = 1
    caching = True
    showpageno = True
    laparams = LAParams()

    laparams.char_margin = char_margin
    laparams.line_margin = line_margin
    laparams.word_margin = word_margin

    #
    PDFDocument.debug = debug
    PDFParser.debug = debug
    CMapDB.debug = debug
    PDFResourceManager.debug = debug
    PDFPageInterpreter.debug = debug
    PDFDevice.debug = debug

    #
    rsrcmgr = PDFResourceManager(caching=caching)

    file_list = [x for x in os.listdir(from_dir) if x.endswith('.pdf')]
    if len(file_list) == 0:
        print 'no .pdf files found'
    else:
        print 'Processing {} .pdf files'.format(len(file_list))

    for i, f in enumerate(file_list):
        t1 = time.time()
        outfile = ''.join([to_dir, f.replace('.pdf', '.txt')])
        fname = ''.join([from_dir, f])
        string = '    {} to text ...'.format(f)
        print

        outfp = file(outfile, 'w')

        device = TextConverter(rsrcmgr, outfp, codec=codec, laparams=laparams, imagewriter=imagewriter)

        fp = file(fname, 'rb')

        interpreter = PDFPageInterpreter(rsrcmgr, device)

        try:
            for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password, caching=caching, check_extractable=True):
                page.rotate = (page.rotate + rotation) % 360
                interpreter.process_page(page)
            print '{0}:'.format(i) + string + ' Completed! {0:.2f} seconds'.format(time.time() - t1)
        except:
            print string + ' - ERROR ENCOUNTERED'
        fp.close()
        device.close()
        outfp.close()
    return


def convert_pdf(args):
    i, arg = args
    fname, from_dir, to_dir = arg

    # debug option
    debug = 0
    # input option
    password = ''
    pagenos = set()
    maxpages = 0
    outtype = "text"
    imagewriter = None
    rotation = 0
    layoutmode = 'normal'
    codec = 'utf-8'
    # pageno = 1
    scale = 1
    caching = True
    showpageno = True
    laparams = LAParams()

    #
    PDFDocument.debug = debug
    PDFParser.debug = debug
    CMapDB.debug = debug
    PDFResourceManager.debug = debug
    PDFPageInterpreter.debug = debug
    PDFDevice.debug = debug

    #
    rsrcmgr = PDFResourceManager(caching=caching)

    t1 = time.time()
    outfile = ''.join([to_dir, fname.replace('.pdf', '.txt')])
    fname = ''.join([from_dir, fname])
    string = '{0}: {1} to text...'.format(i + 1, fname)

    outfp = file(outfile, 'w')

    device = TextConverter(rsrcmgr, outfp, codec=codec, laparams=laparams, imagewriter=imagewriter)

    fp = file(fname, 'rb')

    interpreter = PDFPageInterpreter(rsrcmgr, device)

    try:
        for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password, caching=caching, check_extractable=True):
            page.rotate = (page.rotate + rotation) % 360
            interpreter.process_page(page)
        print string + ' Completed! {0:.2f} seconds'.format(time.time() - t1)
    except:
        print string + ' - ERROR ENCOUNTERED'
    fp.close()
    device.close()
    outfp.close()
    return


def ocr_docs(directory):
    files = [directory + f for f in os.listdir(directory) if f.endswith('.pdf')]
    for i, f in enumerate(files):
        print '\nWorking on file {0} of {1}...'.format(i + 1, len(files))
        ocr_main(f)


def multi_convert_pdfs(from_dir, to_dir):
    pool = Pool(processes=cpu_count() - 1)
    files = [x for x in os.listdir(from_dir) if x.endswith('.pdf')]
    lst = range(len(files))
    args = [(x, from_dir, to_dir) for x in files]
    args = zip(lst, args)
    pool.map(convert_pdf, args)


def test_convert():
    convert_pdfs('welddocs/', 'welddocs/', '.pdf')

if __name__ == '__main__':
    pass
