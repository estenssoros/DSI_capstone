import os
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfdevice import PDFDevice, TagExtractor
from pdfminer.pdfpage import PDFPage
from pdfminer.converter import XMLConverter, HTMLConverter, TextConverter
from pdfminer.cmapdb import CMapDB
from pdfminer.layout import LAParams
from pdfminer.image import ImageWriter


def convert(fname, from_dir, to_dir, m=1.0, w=0.2, l=0.3):
    debug = 0
    password = ''
    pagenos = set()
    maxpages = 0

    outfile = '{0}/{1}'.format(to_dir, fname.replace('.pdf', '.txt'))
    outtype = 'text'

    imagewriter = None
    rotation = 0

    layoutmode = 'normal'
    codec = 'utf-8'
    caching = True
    laparams = LAParams()

    laparams.char_margin = m
    laparams.line_margin = l
    laparams.word_margin = w

    PDFDocument.debug = debug
    PDFParser.debug = debug
    CMapDB.debug = debug
    PDFResourceManager.debug = debug
    PDFPageInterpreter.debug = debug
    PDFDevice.debug = debug

    rsrcmgr = PDFResourceManager(caching=caching)

    outfp = file(outfile, 'w')

    device = TextConverter(rsrcmgr, outfp, codec=codec, laparams=laparams, imagewriter=imagewriter)

    fp = file('{0}/{1}'.format(from_dir, fname), 'rb')

    interpreter = PDFPageInterpreter(rsrcmgr, device)
    for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password, caching=caching, check_extractable=True):
        page.rotate = (page.rotate + rotation) % 360
        interpreter.process_page(page)

    fp.close()
    device.close()
    outfp.close()

    print '{0} written to {1}'.format(fname, to_dir)
    return

if __name__ == '__main__':
    from_dir = 'test_pdf'
    files = os.listdir(from_dir)
    to_dir = 'textdocs'
    for fname in files:
        convert(fname, from_dir, to_dir)
