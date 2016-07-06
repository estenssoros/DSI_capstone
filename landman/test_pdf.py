import PyPDF2
import os
# import slate
import textract

def try_pypdf2(pdfs):
    for pdf in pdfs:
        pdfFileObj = open(pdf, 'rb')
        pdfReader = PyPDF2.PdfFileReader(pdfFileObj)

        num_pages = pdfReader.numPages
        print num_pages
        for page in range(num_pages):
            pageObj = pdfReader.getPage(0)
            text = pageObj.extractText()
            print text
        break
def try_slate(pdfs):
    for pdf in pdfs:
        with open(pdf) as f:
            doc = slate.PDF(f)
        break
    return doc


def try_textract(pdfs):
    pdf_dict = {}
    for pdf in pdfs:
        print pdf
        text = textract.process(pdf, method='tesseract')
        pdf_dict[pdf]= ' '.join(text.split())
    return pdf_dict
if __name__ == '__main__':
    pdf_dir = 'test_pdf/'
    pdfs = [pdf_dir + x for x in os.listdir(pdf_dir)]
    pdf_dict = try_textract(pdfs)
