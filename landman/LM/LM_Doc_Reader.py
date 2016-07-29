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
import time
import os


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

    rsrcmgr = PDFResourceManager(caching=caching)

    t1 = time.time()

    string = '{0}: {1} to text...'.format(i + 1, fname)

    from_file = ''.join([from_dir, fname])
    fp = file(from_file, 'rb')

    if from_dir == 'ocrdocs/':
        fname = fname.replace('_ocr', '')

    outfile = ''.join([to_dir, fname.replace('.pdf', '.txt')])
    outfp = file(outfile, 'w')

    device = TextConverter(rsrcmgr, outfp, codec=codec, laparams=laparams, imagewriter=imagewriter)

    interpreter = PDFPageInterpreter(rsrcmgr, device)

    try:
        for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password, caching=caching, check_extractable=True):
            page.rotate = (page.rotate + rotation) % 360
            interpreter.process_page(page)
        print string + ' Completed! {0:.2f} seconds'.format(time.time() - t1)
    except Exception as e:
        print string + ' - ERROR ENCOUNTERED: {0}'.format(e)
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
    pool = Pool(processes=cpu_count() - 1, maxtasksperchild=100)
    files = [x for x in os.listdir(from_dir) if x.endswith('.pdf')]
    lst = range(len(files))
    args = [(x, from_dir, to_dir) for x in files]
    args = zip(lst, args)
    pool.map(convert_pdf, args)
    pool.close()
    pool.join()


if __name__ == '__main__':
    pass
