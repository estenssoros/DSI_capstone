from LM_AWS import get_docs_from_s3, write_all_to_s3, connect_s3
from LM_Util import clear_docs_from_dict, twilio_message
from LM_Doc_Reader import multi_convert_pdfs
from multiprocessing import cpu_count, Pool
from string import maketrans, punctuation
import pandas as pd


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
        ttl = 5
        extract_text(cpu_count() * ttl, s3_dir)
    twilio_message('read {0} text docs to s3'.format(loops * cpu_count() * ttl))


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


def multi_doc(func, cols):
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
    pool.close()
    pool.join()
    print 'done!'
    return pd.DataFrame(results, columns=cols)


def replace_word(word, repl):
    with open('words_by_frequency.txt') as f:
        text = f.read()

    text = text.replace(word, repl)

    with open('words_by_frequency.txt', 'w') as f:
        f.write(text)

    print 'done!'


def condense(lst):
    lst = [''.join(x.split()) for x in lst]
    return ''.join(lst)


def reg_key_words(key_dict):
    results = defaultdict(list)

    for k, v in key_dict.iteritems():
        if k == 'township':
            results[k].append(re.findall('[0-9]+n', v))
        if k == 'section':
            results[k].append(re.findall('[0-9]+', v))
        if k == 'range':
            results[k].append(re.findall('[0-9]+w', v))
        if k == 'landincludedinlease':
            results[k].append([v])
        if k == 'descriptionofland':
            results[k].append([v])

    return results

def find_words(args, words=None, maxword=None, keywords=None):
    '''

    '''
    doc, text = args

    if words is None:
        with open('text/words_by_frequency.txt') as f:
            words = set(f.read().lower().split())

    if maxword is None:
        maxword = max(len(x) for x in words)

    if keywords is None:
        with open('text/keywords.txt') as f:
            keywords = set(f.read().lower().split())

    maxword = max(max(len(x) for x in keywords), maxword)

    results = []
    found = []
    while len(text) > 0:
        start = 0
        end = start + 1
        options = []
        for i in range(maxword):
            test_word = text[start:end]
            if test_word in words:
                options.append(test_word)
                if test_word in keywords:
                    found.append(test_word)
            end += 1

        if options:
            option = max(options, key=lambda x: len(x))
            text = text[len(option):]
            results.append(option)
        else:
            text = text[1:]

    # find index location of key words in main text
    lst = list(results)
    indexes = []
    for word in found:
        idx = lst.index(word)
        indexes.append(idx)
        lst[idx] = ""

    # pull folowing text of key words
    df = pd.DataFrame(columns=['doc', 'township', 'section', 'range'])
    key_dict = {}
    for i in range(len(found) - 1):
        key_lst = results[indexes[i] + 1:indexes[i + 1]]
        if len(key_lst) > 10:
            key_lst = key_lst[:10]
        if found[i] in key_dict:
            key_dict[found[i]].append(''.join(key_lst))
        else:
            key_dict[found[i]] = [''.join(key_lst)]

    # for k, v in key_dict.iteritems():
    #     key_dict[k] = [condense(x) for x in v]

    # options = ['ne\d*', 'nw\d*', 'se\d*', 'sw\d*', 'n\d*', 'n\d*', 'all']
    # quarters = []
    # if 'section' in key_dict:
    #     for i, section in enumerate(key_dict['section']):
    #         section_qtr = []
    #         for option in options:
    #             re_find = re.findall(option, section)
    #             if len(re_find) > 0:
    #                 section_qtr.append(re_find)
    #                 for x in re_find:
    #                     section = key_dict['section'][i].replace(x, '')
    #         key_dict['section'][i] = section
    #
    #         if section_qtr:
    #             quarters.append((i, section_qtr))
    # key_dict = reg_key_words(key_dict)
    # if quarters:

    return doc, ' '.join(results), key_dict
