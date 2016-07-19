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


def convert_pdfs(from_dir, to_dir, ext):
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

    laparams.char_margin = float(2.0)
    laparams.line_margin = float(0.5)
    laparams.word_margin = float(0.1)

    #
    PDFDocument.debug = debug
    PDFParser.debug = debug
    CMapDB.debug = debug
    PDFResourceManager.debug = debug
    PDFPageInterpreter.debug = debug
    PDFDevice.debug = debug

    #
    rsrcmgr = PDFResourceManager(caching=caching)

    file_list = [x for x in os.listdir(from_dir) if x.endswith(ext)]
    if len(file_list) == 0:
        print 'no .pdf files found'
    else:
        print 'Processing {} .pdf files'.format(len(file_list))

    for f in file_list:
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
            print string + ' Completed! {0:.2f} seconds'.format(time.time() - t1)
        except:
            print string + ' - ERROR ENCOUNTERED'
        fp.close()
        device.close()
        outfp.close()
    return


def ocr_docs(directory):
    pool = Pool(processes=cpu_count())
    files = [directory + f for f in os.listdir(directory) if f.endswith('.pdf')]
    pool.map(ocr_main, files)


def test_convert():
    convert_pdfs('welddocs/', 'welddocs/', '.pdf')

if __name__ == '__main__':
    test_convert()
