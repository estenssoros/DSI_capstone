from LM_AWS import get_docs_from_s3, write_all_to_s3
from LM_Doc_Reader import ocr_docs
from LM_Util import rename_files, clear_docs_from_dict, read_json, twilio_message
from multiprocessing import cpu_count
import os


def extract_ocr(limit):
    '''
    INPUT: #
    OUTPUT: None
    Automates process of downloaded documents form s3, running OCR, uploading
    OCR documents to s3, and removing documents from local machine.
    '''
    pdf_dir = 'welddocs/'
    ocr_dir = 'ocrdocs/'

    get_docs_from_s3(limit, pdf_dir, '.pdf', 'converted')
    ocr_docs(pdf_dir)
    rename_files('_ocr.pdf', pdf_dir, ocr_dir)

    write_all_to_s3('.pdf', ocr_dir)
    clear_docs_from_dict({'.pdf': [pdf_dir, ocr_dir]})

def loop_ocr(loops):
    '''
    INPUT: #
    OUTPUT: None
    Runs extract_ocr a loops # of times and sends a twillio message when complete
    '''
    for i in range(loops):
        print '\n\n-------------- LOOP: {0}/{1} --------------'.format(i + 1, loops)
        limit = cpu_count() * 10
        # limit = 3
        extract_ocr(limit)
    ip = os.uname()[1]
    try:
        ips = read_json('ip.json')
        if ip in ips.keys():
            ip = ips[ip]
    except:
        pass

    count = loops * limit
    twilio_message('Done! Processed: {0} from  {1}'.format(count, ip))
