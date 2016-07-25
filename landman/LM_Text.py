from LM_AWS import get_docs_from_s3, write_all_to_s3
from LM_Util import clear_docs_from_dict, twilio_message
from LM_Doc_Reader import multi_convert_pdfs
from multiprocessing import cpu_count


def extract_text(limit, s3_dir):
    '''
    INPUT: integer
    OUTPUT: None
    Reads pdf docs from s3 and converts to text
    '''
    get_docs_from_s3(limit, s3_dir, '.pdf', df_col='text')
    multi_convert_pdfs(s3_dir, 'textdocs/')
    write_all_to_s3('.txt', 'textdocs/')
    clear_docs_from_dict({'.pdf': [s3_dir], '.txt': ['textdocs/']})


def loop_text(loops):
    '''
    INPUT: integer
    OUTPUT: None
    Loops extract_text() and sends twillio message when complete
    '''
    for i in range(loops):
        print '----------------- LOOP: {0}/{1} -----------------'.format(i + 1, loops)
        extract_text(cpu_count() * 5, 'ocrdocs/')
    twilio_message('read {0} text docs to s3'.format(loops * cpu_count() * 12))
