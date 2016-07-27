from __future__ import division
from twilio.rest import TwilioRestClient
from pymongo import MongoClient
from bs4 import BeautifulSoup
from dateutil import parser
import datetime as dt
import pandas as pd
import mechanize
import cookielib
import json
import time
import boto
import os
import re


class LandMan(object):
    '''
    He does everything a Landman can!
    '''

    def __init__(self):
        self.keys = self._get_keys()
        self.urls = self._get_urls('url.json')
        self.coll = self._mongo()
        self.br = self._start_browser()
        self.start_date = dt.datetime(2006, 1, 1)
        self.end_date = self.start_date + dt.timedelta(days=1)
        self.search = self._search_weld()

    def _get_keys(self):
        keys = {}
        env = os.environ
        keys['aws'] = {'aws_key': env['AWS_ACCESS_KEY_ID'], 'aws_secret': env['AWS_SECRET_ACCESS_KEY']}
        keys['weld_c'] = {'userId': env['WELD_COUNTY_USER_NAME'], 'password': env['WELD_COUNTY_PASSWORD']}
        keys['twilio'] = {'account': env['TWILIO_ACCOUNT'], 'token': ['TWILIO_TOKEN']}

    def _get_urls(self, fname):
        with open(fname) as f:
            for line in f:
                urls = json.loads(line)
        return urls

    def _mongo(self):
        client = MongoClient()
        db = client['landman']
        coll = db['weld_county']
        return coll

    def _start_browser(self):
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

        br.open(self.urls['login'])
        br.select_form(nr=1)

        br.form['userId'] = self.keys['weld_c']['userId']
        br.form['password'] = self.keys['weld_c']['password']
        br.submit()

        print 'Form submitted! Arrived at {}'.format(br.title())
        return br

    def _test_html(self, string):
        if string in html:
            return False
        else:
            return True

    def _search_weld(self, search_count=1):
        while True:
            self.br.select_form(nr=0)

            select_items = ['ABSTRACTLSE', 'MEMOLSE', 'OGLSE', 'OGLSEASN']

            cntrls = {'RecordingDateIDStart': self.start_date.strftime('%m/%d/%Y'),
                      'RecordingDateIDEnd': self.end_date.strftime('%m/%d/%Y'),
                      'AllDocuments': False,
                      '__search_select': select_items}

            for k, v in cntrls.iteritems():
                self.br.form[k] = v
            req = self.br.submit()
            html = req.read()
            if test_html(html, 'No results found'):
                print '      - start:', self.start_date.strftime('%m/%d/%Y')
                print '      - end:  ', self.end_date.strftime('%m/%d/%Y')
                yield
            else:
                print '      - No records found'
                print '     ----------------------'
            self.br.back()
            self.start_date = self.end_date
            search_count += 1

    def _parse_html(self, html, i):
        soup = BeautifulSoup(html, 'html.parser')
        page_banner = soup.find_all('span', attrs={'class': 'pagebanner'})[0].get_text()
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

    def _get_dates(self):
        return max([parser.parse(row['end_date']) for row in self.coll.find()])

    def _recursive_br(self, k=0):
        html = self.br.response().read()
        results = self._parse_html(html, k)
        k += len(results)

        found_next = False
        for link in br.links():
            if link.text == 'Next':
                found_next = True
                break

        if found_next == True:
            print 'switching to:', link.url
            self.br.follow_link(link)
            results.update(self._recursive_br(k))
            self.br.back()
        return results

    def get_doc_numbers(self):
        try:
            self.start_date = self._get_dates()
        except Exception as e:
            print 'No mongo data, starting from {0}'.format(self.start_date)
        for i in range(30):
            print '_____________________|' + str(i) + '|_____________________'
            try:
                self.search.next()
            except:
                self.br = self._start_browser()
                self.start_date = get_dates(coll)
                self.search = self._search_weld()
                self.search.next()
        results = self.recursive_br()
        mongo_d = {}
        mongo_d['start_date'] = str(self.start_date.date())
        mongo_d['end_date'] = str(self.end_date.date())
        mongo_d['results'] = results
        print '      - {0} records(s) found'.format(len(results))
        print ''

        self.coll.insert_one(mongo_d)
        if len(results) > 100:
            for link in self.br.links():
                if link.text == 'Modify Search':
                    self.br.follow_link(link)
                    break

    def download_docs(self, limit, directory):
        '''
        INPUT: scraping limit, save directory
        OUTPUT: None

        '''
        base_url = self.urls['doc_url']
        if not directory.endswith('/'):
            directory += '/'

        df = pd.read_csv('data/clean_weld_docs.csv', dtype=object)
        read = pd.read_csv('data/new_read.csv', dtype=object)

        print '-------------------{0}-------------------'.format(len(read))

        doc_nums = df['new_doc_num'][~df['new_doc_num'].isin(read['new_doc_num'].values.tolist())].values.tolist()

        self.br = self._start_browser()

        for i, doc in enumerate(doc_nums):
            doc_id = 0
            print '{}: {}_{}'.format(i, doc, doc_id)
            url = base_url + doc
            try:
                self.br.open(url)
            except Exception as e:
                message = 'Script encountered error with browser object: {} '.format(e)
                message += '{0} total files in new_read.csv'.fromat(len(read))
            try:
                for link in self.br.links():
                    if 'view attachment' in link.text.lower():
                        self.br.retrieve(link.absolute_url, directory + str(doc) + '_' + str(doc_id) + '.pdf')
                        read = read.append({'new_doc_num': str(doc),
                                            'doc_id': doc_id}, ignore_index=True)
                        read.to_csv('data/new_read.csv', index=False)
                        doc_id += 1

            except Exception as e:
                print e

            if i == limit:
                print '{0} documents read...'.format(len(read))
                break

    def _write_to_s3(self, fname, directory=None):
        conn = boto.connect_s3(self.keys['aws']['aws_key'], self.keys['aws']['aws_secret'])
        bucket_name = 'sebsbucket'
        if conn.lookup(bucket_name) is None:
            raise ValueError('Bucket does not exist! WTF!')
        else:
            b = conn.get_bucket(bucket_name)

        if directory:
            fname = directory + fname

        file_object = b.new_key(fname)
        file_object.set_contents_from_filename(fname, policy='public-read')

        print '{} written to {}!'.format(fname, bucket_name)

    def upload_docs(self, directory):
        if not directory.endswith('/'):
            directory += '/'
        for f in os.listdir(directory):
            if f.endswith('.pdf'):
                write_to_s3(f, directory)
                os.remove(directory + f)

    def _print_status(self, j, i, t_1, t_2):
        j += 1
        i += 1
        t_1 = (time.time() - t_1) / 60
        t_2 = (time.time() - t_2) / 60
        print '{0}/50 - {1}/5 - sub time: {2:.2f} - total time: {3:.2f}'.format(j, i, t_2, t_1)

    def _twilio_message(self, message):
        '''
        INPUT: message
        OUTPUT: None
        Sends SMS via twilio client
        '''
        client = TwilioRestClient(self.keys['twilio']['account'], self.keys['twilio']['token'])
        message = client.messages.create(to="+13032299207", from_="+17206139570", body=message)

    def _sync_read(self):
        df = pd.read_csv('https://s3.amazonaws.com/sebsbucket/data/new_read.csv')
        df.to_csv('data/new_read.csv', index=False)

    def get_all_docs(self):
        self._sync_read()
        t_1 = time.time()
        for j in range(50):
            for i in range(5):
                t_2 = time.time()
                self.download_docs(49, 'welddocs/')
                sel._print_status(j, i, t_1, t_2)

                self.upload_docs('welddocs/')
                write_to_s3('data/new_read.csv')
                self._print_status(j, i, t_1, t_2)
            time.sleep(60)
        self._twilio_message('Python script done!')
