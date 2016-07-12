import mechanize
import cookielib
from bs4 import BeautifulSoup
import os
import datetime as dt
import json
import time
from pymongo import MongoClient
import re
from dateutil import parser
import pandas as pd


def get_url(fname):
    '''
    Read url form json file
    '''
    with open(fname) as f:
        for line in f:
            url = json.loads(line)
    return url['url']


def start_browser(fname='url.json'):
    '''
    Spin up mechannize browser with starting url from json file
    '''
    url = get_url(fname)
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

    br.open(url)
    br.select_form(nr=1)

    br.form['userId'] = os.environ['WELD_COUNTY_USER_NAME']
    br.form['password'] = os.environ['WELD_COUNTY_PASSWORD']
    br.submit()
    print 'Form submitted! Arrived at {}'.format(br.title())
    return br


def test_html(html):
    '''
    Test to see if html returned results
    '''
    bad_search = 'No results found'
    if bad_search in html:
        return False
    else:
        return True


def search_weld(br, start_date, search_count=1):
    '''
    Mechanize generator that pulls html from queries from Weld CC website
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
        if test_html(html):
            print '      - start:', start_date.strftime('%m/%d/%Y')
            print '      - end:  ', end_date.strftime('%m/%d/%Y')
            yield html, start_date, end_date, br
        else:
            print '      - No records found'
            print '     ----------------------'
        br.back()
        start_date = end_date
        search_count += 1
        # time.sleep(0.5)


def parse_html(html, i):
    '''
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
        result['doc_num'] = ''.join(re.findall(r'[0-9]', href))
        result['text'] = tds[1].get_text()
        results[str(i)] = result
        i += 1

    return results


def get_dates(coll):
    '''
    Get latest date from mongodb
    '''
    dates = [parser.parse(row['end_date']) for row in coll.find()]
    return max(dates)


def recursive_br(br, k=0):
    '''
    Recursive function to find all pages associate with a websearch
    RETURNS:
    - a dictionary of web results
    '''
    # read html and get results
    html = br.response().read()
    results = parse_html(html, k)
    k += len(results)

    # find next link if exists
    found_next = False
    for link in br.links():
        if link.text == 'Next':
            found_next = True
            break
    # follow next link if exists
    if found_next == True:
        print 'switching to:', link.url
        # time.sleep(0.5)
        br.follow_link(link)
        # recursion
        results.update(recursive_br(br, k))
        br.back()
    return results


def run_scraper(br, coll, start_date=dt.datetime(2006, 1, 1)):
    '''
    TO DO:
    - add multiprocessing and threading?
    - make doc_num into mongo _id?
    '''
    try:
        start_date = get_dates(coll)
    except Exception as e:
        print 'No mongo data, starting from {0}'.format(start_date)

    # create serach generator
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


def get_doc_numbers():
    client = MongoClient()
    db = client['landman']
    coll = db['weld_county']
    br = start_browser()
    br = run_scraper(br, coll)


def read_from_s3(fname):
    df = pd.read_csv(fname)
    return df
if __name__ == '__main__':
    df = pd.read_csv('data/clean_weld_docs.csv',dtype=object)
    read = pd.read_csv('data/read_docs.csv', dtype=object)
    doc_nums = [d for d in df['doc_num'] if d not in read['doc_num']]

    br = start_browser()

    for i, doc in enumerate(doc_nums):
        print doc
        url = 'https://searchicris.co.weld.co.us/recorder/eagleweb/viewDoc.jsp?node=DOCC' + str(doc)

        br.open(url)

        for link in br.links():
            if 'view attachment' in link.text.lower():
                br.retrieve(link.absolute_url, 'docs/' + str(doc) + '.pdf')
                read = read.append({'doc_num':str(doc)},ignore_index=True)
                read.to_csv('data/read_docs.csv',index=False)
        if i >5:
            break
