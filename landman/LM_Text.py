from LM_AWS import get_docs_from_s3, write_all_to_s3
from LM_Util import clear_docs_from_dict, twilio_message
from LM_Doc_Reader import multi_convert_pdfs
from multiprocessing import cpu_count, Pool
from string import maketrans, punctuation

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


def loop_text(loops, s3_dir='ocrdocs/'):
    '''
    INPUT: integer
    OUTPUT: None
    Loops extract_text() and sends twillio message when complete
    '''
    for i in range(loops):
        print '----------------- LOOP: {0}/{1} -----------------'.format(i + 1, loops)
        extract_text(cpu_count() * 5, s3_dir)
    twilio_message('read {0} text docs to s3'.format(loops * cpu_count() * 12))


def word_count_docs(key):
    '''
    INPUT: aws s3 key object
    OUTPUT: doc number, word count of text, size of file
    '''
    text = key.get_contents_as_string()
    text = text.replace('\n', ' ')
    w_count = len(text.split())
    doc = key.name.replace('textdocs/', '').replace('.txt', '')
    size = key.size
    return doc, w_count, size


def clean_docs(key):
    '''
    INPUT: aws s3 key object
    OUTPUT: doc number, text from file
    '''
    text = key.get_contents_as_string()
    spaces = ' ' * len(punctuation)
    table = maketrans(punctuation, spaces)
    text = text.translate(table)
    replace = ['\n', '\x0c']
    for r in replace:
        text = text.replace(r, ' ')
    doc = key.name.replace('textdocs/', '').replace('.txt', '')
    return doc, text


def multi_word_count(func, cols):
    '''
    INPUT: function, list of columns
    OUTPUT: dataframe
    '''
    b = connect_s3()
    keys = [key for key in list(b.list('textdocs/')) if key.name.endswith('.txt')]
    print 'keys read in!'
    print 'beggining document analysis!'
    pool = Pool(processes=cpu_count() - 1)
    results = pool.map(func, keys)
    print 'done!'
    return pd.DataFrame(results, columns=cols)
