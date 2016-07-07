import mechanize
import cookielib
from bs4 import BeautifulSoup
import os
import datetime as dt
import json
import time
from pymongo import MongoClient
import re
import threading
from dateutil import parser


def get_url(fname):
    '''
    Read url form json file
    '''
    with open(fname) as f:
        for line in f:
            url = json.loads(line)
    return url['url']


def start_browser(fname):
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
        print '------------------' + str(search_count) + '------------------'
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
            print '    start:', start_date.strftime('%m/%d/%Y')
            print '    end:  ', end_date.strftime('%m/%d/%Y')
            yield html, start_date, end_date
        br.back()
        start_date = end_date
        search_count += 1
        time.sleep(0.5)


def parse_html(html):
    '''
    Parse html with BeautifulSoup and return dictionary of dictionary of table results
    TO DO:
    - add try and excepts to all ".find_all"
    - test to see if span class='pagelinks' exists
        - loop through all pages
    '''
    soup = BeautifulSoup(html, 'html.parser')
    found_all = True
    page_banner = soup.find_all('span', attrs={'class': 'pagebanner'})[
        0].get_text()

    if '[First/Prev]' in page_banner:
        found_all = False

    pages = ''.join(re.findall(r'[0-9]', page_banner))

    table = soup.find_all('table', attrs={'id': 'searchResultsTable'})[0]
    rows = table.find_all('tr', {'class': ['even', 'odd']})
    results = {}
    for i, row in enumerate(rows):
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

    return results, found_all


def get_dates(coll):
    dates = [parser.parse(row['end_date']) for row in coll.find()]
    return max(dates)


def run_scraper(br, coll, start_date=dt.datetime(2006, 1, 1)):
    '''
    TO DO:
    - add multiprocessing and threading
    - make doc_num into mongo _id
    '''
    try:
        start_date = get_dates(coll)
    except Exception as e:
        print e

    search = search_weld(br, start_date)

    for i in range(30):
        mongo_d = {}
        html, start_date, end_date = search.next()
        results, found_all = parse_html(html)
        # mongo_d['html'] = html
        mongo_d['start_date'] = str(start_date.date())
        mongo_d['end_date'] = str(end_date.date())
        mongo_d['results'] = results
        mongo_d['found_all'] = found_all
        print '    -> {0} result(s) found'.format(len(results))
        if found_all==False:
            print 'WARNING: Missing Entries From Query'

        try:
            coll.insert_one(mongo_d)
        except Exception as e:
            print e

if __name__ == '__main__':
    client = MongoClient()
    db = client['landman']
    coll = db['weld_county']
    br = start_browser('url.json')
    run_scraper(br, coll)
