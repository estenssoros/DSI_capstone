from __future__ import division
from doc_reader import convert_pdfs, ocr_docs, multi_convert_pdfs
from twilio.rest import TwilioRestClient
from multiprocessing import cpu_count
from collections import Counter
from pymongo import MongoClient
from bs4 import BeautifulSoup
from dateutil import parser
import datetime as dt
import pandas as pd
import numpy as np
import mechanize
import cookielib
import json
import time
import boto
import os
import re


def read_json(fname, url_name):
    '''
    INPUT: file name, dictionary key
    OUTPUT: url form dictionary
    Read url form json file
    '''
    with open(fname) as f:
        for line in f:
            urls = json.loads(line)
    return urls[url_name]


def start_browser(fname='url.json'):
    '''
    INPUT: url.json filename
    OUTPUT: mechanize browser
    Initialized mechanize browser with starting url from json file
    '''

    br = mechanize.Browser(factory=mechanize.RobustFactory())

    cj = cookielib.LWPCookieJar()
    br.set_cookiejar(cj)

    br.set_handle_equiv(True)
    br.set_handle_redirect(True)
    br.set_handle_referer(True)
    br.set_handle_robots(False)

    br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

    br.addheaders = [
        ('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]

    url = read_json(fname, 'login')
    br.open(url)
    br.select_form(nr=1)

    br.form['userId'] = os.environ['WELD_COUNTY_USER_NAME']
    br.form['password'] = os.environ['WELD_COUNTY_PASSWORD']
    br.submit()

    print 'Form submitted! Arrived at {}'.format(br.title())
    return br


def test_html(html, string):
    '''
    INPUT: html
    OUTPUT: boolean

    Test to see if html returned results
    '''
    if string in html:
        return False
    else:
        return True


def search_weld(br, start_date, search_count=1):
    '''
    INPUT: mehcnize browser, start_date, search_count
    OUTPUT: yields html, start_date, end_date, and mechanize browser
    Generator that queries and pulls html from Weld CC website
    '''
    while True:
        br.select_form(nr=0)
        end_date = start_date + dt.timedelta(days=1)

        select_items = ['ABSTRACTLSE', 'MEMOLSE', 'OGLSE', 'OGLSEASN']

        cntrls = {'RecordingDateIDStart': start_date.strftime('%m/%d/%Y'),
                  'RecordingDateIDEnd': end_date.strftime('%m/%d/%Y'),
                  'AllDocuments': False,
                  '__search_select': select_items}

        for k, v in cntrls.iteritems():
            br.form[k] = v
        req = br.submit()
        html = req.read()
        if test_html(html, 'No results found'):
            print '      - start:', start_date.strftime('%m/%d/%Y')
            print '      - end:  ', end_date.strftime('%m/%d/%Y')
            yield html, start_date, end_date, br
        else:
            print '      - No records found'
            print '     ----------------------'
        br.back()
        start_date = end_date
        search_count += 1


def parse_html(html, i):
    '''
    INPUT: html, dictionary key
    OUTPUT: documents metadata from html

    Parse html with BeautifulSoup and return dictionary of dictionary of table results
    '''
    soup = BeautifulSoup(html, 'html.parser')
    page_banner = soup.find_all('span', attrs={'class': 'pagebanner'})[
        0].get_text()
    pages = ''.join(re.findall(r'[0-9]', page_banner))

    table = soup.find_all('table', attrs={'id': 'searchResultsTable'})[0]
    rows = table.find_all('tr', {'class': ['even', 'odd']})

    results = {}
    for row in rows:
        result = {}
        tds = row.find_all('td')
        a = tds[0].find_all('a')[0]
        href = a['href']
        doc_type = a.get_text()
        result['href'] = href
        result['doc_type'] = doc_type
        result['doc_num'] = href[href.find('DOC'):]
        result['text'] = tds[1].get_text()
        results[str(i)] = result
        i += 1

    return results


def get_dates(coll):
    '''
    INPUT: mongo collection object
    OUTPUT: highest date in mongo colection

    Get latest date from mongodb
    '''
    dates = [parser.parse(row['end_date']) for row in coll.find()]
    return max(dates)


def recursive_br(br, k=0):
    '''
    INPUT: mechanize browser, results dictionary key
    OUTPUT: dictionary of web results

    Recursive function to find all pages associate with a websearch
    '''

    html = br.response().read()
    results = parse_html(html, k)
    k += len(results)

    found_next = False
    for link in br.links():
        if link.text == 'Next':
            found_next = True
            break

    if found_next == True:
        print 'switching to:', link.url
        br.follow_link(link)
        results.update(recursive_br(br, k))
        br.back()
    return results


def get_doc_numbers(start_date=dt.datetime(2006, 1, 1)):
    '''
    INPUT: None
    OUTPUT: None

    Searches weld county website for document numbers populates into mongo database
    '''

    client = MongoClient()
    db = client['landman']
    coll = db['weld_county']

    br = start_browser()

    try:
        start_date = get_dates(coll)
    except Exception as e:
        print 'No mongo data, starting from {0}'.format(start_date)

    # create search generator
    search = search_weld(br, start_date)

    for i in range(30):

        print '_____________________|' + str(i) + '|_____________________'

        # next generator
        try:
            html, start_date, end_date, br = search.next()
        except:
            br = start_browser()
            start_date = get_dates(coll)
            search = search_weld(br, start_date)
            html, start_date, end_date, br = search.next()

        # call recursive browser
        results = recursive_br(br)

        # populatie mongo dictionary
        mongo_d = {}
        mongo_d['start_date'] = str(start_date.date())
        mongo_d['end_date'] = str(end_date.date())
        mongo_d['results'] = results
        print '      - {0} records(s) found'.format(len(results))
        print ''

        coll.insert_one(mongo_d)
        if len(results) > 100:
            for link in br.links():
                if link.text == 'Modify Search':
                    br.follow_link(link)
                    break


def download_docs(limit, directory):
    '''
    INPUT: scraping limit, save directory
    OUTPUT: None

    '''
    base_url = read_json('url.json', 'doc_url')
    if not directory.endswith('/'):
        directory += '/'

    df = pd.read_csv('data/clean_weld_docs.csv', dtype=object)
    read = pd.read_csv('data/new_read.csv', dtype=object)

    print '-------------------{0}-------------------'.format(len(read))

    doc_nums = df['new_doc_num'][~df['new_doc_num'].isin(read['new_doc_num'].values.tolist())].values.tolist()

    br = start_browser()

    for i, doc in enumerate(doc_nums):
        doc_id = 0
        print '{}: {}_{}'.format(i, doc, doc_id)
        url = base_url + doc
        try:
            br.open(url)
        except Exception as e:
            message = 'Script encountered error with browser object: {} '.format(e)
            message += '{0} total files in new_read.csv'.fromat(len(read))
        try:
            for link in br.links():
                if 'view attachment' in link.text.lower():
                    br.retrieve(link.absolute_url, directory + str(doc) + '_' + str(doc_id) + '.pdf')
                    read = read.append({'new_doc_num': str(doc),
                                        'doc_id': doc_id}, ignore_index=True)
                    read.to_csv('data/new_read.csv', index=False)
                    doc_id += 1

        except Exception as e:
            print e

        if i == limit:
            print '{0} documents read...'.format(len(read))
            break


def get_aws_keys():
    '''
    INPUT: None
    OUTPUT: aws access_key and secret_access_key
    '''
    env = os.environ
    access_key = env['AWS_ACCESS_KEY_ID']
    access_secret_key = env['AWS_SECRET_ACCESS_KEY']
    return access_key, access_secret_key


def connect_s3():
    access_key, access_secret_key = get_aws_keys()

    conn = boto.connect_s3(access_key, access_secret_key)
    bucket_name = 'sebsbucket'

    if conn.lookup(bucket_name) is None:
        raise ValueError('Bucket does not exist! WTF!')
    else:
        b = conn.get_bucket(bucket_name)
        print 'Connected to {}!'.format(bucket_name)
    return b


def write_to_s3(fname, directory=None):
    '''
    INPUT: File name, Directory
    OUTPUT: None
    write file at given directory to S3 bucket
    '''
    b = connect_s3()

    if directory:
        fname = directory + fname

    file_object = b.new_key(fname)
    file_object.set_contents_from_filename(fname, policy='public-read')

    print '{} written to {}!'.format(fname, b.name)


def write_all_to_s3(ext, directory):
    b = connect_s3()
    if not directory.endswith('/'):
        directory += '/'
    for fname in os.listdir(directory):
        if fname.endswith(ext):
            write_to = directory + fname
            file_object = b.new_key(write_to)
            file_object.set_contents_from_filename(write_to, policy='public-read')
            print '{0} written to {1}!'.format(write_to, b.name)


def upload_docs(directory, ext):
    '''
    INPUT: directory
    OUTPUT: None
    Passes documents to write_to_s3 and removes from local machine
    '''
    if not directory.endswith('/'):
        directory += '/'
    for f in os.listdir(directory):
        if f.endswith(ext):
            write_to_s3(f, directory)
            os.remove(directory + f)


def print_status(j, i, t_1, t_2):
    '''
    INPUT: main loop index, sub loop index, main time, sub time
    OUTPUT: None
    Print current status and time of scraping loop
    '''
    j += 1
    i += 1
    t_1 = (time.time() - t_1) / 60
    t_2 = (time.time() - t_2) / 60
    print '{0}/50 - {1}/5 - sub time: {2:.2f} - total time: {3:.2f}'.format(j, i, t_2, t_1)


def twilio_message(message):
    '''
    INPUT: message
    OUTPUT: None
    Sends SMS via twilio client
    '''
    account = os.environ['TWILIO_ACCOUNT']
    token = os.environ['TWILIO_TOKEN']
    client = TwilioRestClient(account, token)
    message = client.messages.create(to="+13032299207", from_="+17206139570", body=message)


def sync_read(r=False):
    df = pd.read_csv('https://s3.amazonaws.com/sebsbucket/data/new_read.csv')
    df.to_csv('data/new_read.csv', index=False)
    if r:
        return df


def get_docs():
    '''
    INPUT: None
    OUTPUT: None
    Run loop to scrap weld county website at desired pace. Upload scraped
    documents to S3 bucket and removed from local machine.
    '''
    sync_read()
    t_1 = time.time()
    for j in range(50):
        for i in range(5):
            t_2 = time.time()
            download_docs(49, 'welddocs/')
            print_status(j, i, t_1, t_2)

            upload_docs('welddocs/', '.pdf')
            write_to_s3('data/new_read.csv')
            print_status(j, i, t_1, t_2)
        time.sleep(60)
    twilio_message('Python script done!')


def clear_docs_from_dict(clear_dict):
    for extension, directories in clear_dict.iteritems():
        for directory in directories:
            clear_docs(extension, directory)


def clear_docs(extension, directory):
    print 'removing {0} from {1}'.format(extension, directory)
    for f in os.listdir(directory):
        if f.endswith(extension):
            os.remove(directory + f)


def get_docs_from_s3(limit, s3_dir, ext, df_col=None):
    b = connect_s3()
    if df_col:
        df = sync_read(r=True)
        if df_col not in df.columns:
            df[df_col] = False
        not_read = df['doc'][df[df_col] == False].values.tolist()[:limit]
        m = df['doc'].isin(not_read)
        df.loc[m, df_col] = True
        df.to_csv('data/new_read.csv', index=False)
        write_to_s3('data/new_read.csv')

        for doc in not_read:
            fname = ''.join([s3_dir, doc, ext])
            key = b.new_key(fname)
            key.get_contents_to_filename(fname)
    else:
        for i, key in enumerate(b.list(s3_dir)):
            key.get_contents_to_filename(key.name)
            if i == limit:
                break
    return


def rename_files(ext, from_dir, to_dir):
    for fname in os.listdir(from_dir):
        if fname.endswith(ext):
            os.rename(from_dir + fname, to_dir + fname)


def extract_ocr(limit):
    pdf_dir = 'welddocs/'
    ocr_dir = 'ocrdocs/'

    get_docs_from_s3(limit, pdf_dir, '.pdf', 'converted')
    ocr_docs(pdf_dir)
    rename_files('_ocr.pdf', pdf_dir, ocr_dir)

    write_all_to_s3('.pdf', ocr_dir)
    clear_docs('.pdf', pdf_dir)
    clear_docs('.pdf', ocr_dir)


def progress(s3_dir='ocrdocs/'):
    b = connect_s3()
    files = [1 for f in b.list(s3_dir) if f.name.endswith('.pdf')]
    return len(files)


def loop_it(loops):
    for i in range(loops):
        print '\n\n-------------- LOOP: {0}/{1} --------------'.format(i + 1, loops)
        limit = cpu_count() * 12
        # limit = 3
        extract_ocr(limit)
    count = loops * limit
    twilio_message('Done! Processed: {0} from  {1}'.format(count, os.uname()[1]))


def doc_stats(fname):
    with open(fname) as f:
        text = f.read()
    text = text.lower()
    words = text.split()
    c_words = Counter(words)
    print len(c_words)
    return len(c_words)


def extract_text(limit):
    get_docs_from_s3(limit, 'welddocs/', '.pdf', df_col='text')
    multi_convert_pdfs('welddocs/', 'textdocs/')
    write_all_to_s3('.txt', 'textdocs/')
    clear_docs_from_dict({'.pdf': ['welddocs/'], '.txt': ['textdocs/']})


def loop_text(loops):
    for i in range(loops):
        print '----------------- LOOP: {0}/{1} -----------------'.format(i + 1, loops)
        extract_text(cpu_count() * 12)
    twilio_message('read {0} text docs to s3'.format(loops * cpu_count() * 12))


if __name__ == '__main__':
    pass
