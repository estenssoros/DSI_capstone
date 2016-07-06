import mechanize
import cookielib
from bs4 import BeautifulSoup
import pandas as pd
import os
import datetime as dt
import json
import time


def get_url(fname):
    with open(fname) as f:
        for line in f:
            url = json.loads(line)
    return url['url']


def start_browser(fname):
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
    bad_search = 'No results found'
    if bad_search in html:
        return False
    else:
        return True


def search_weld(br, start_date, search_count=1):

    while True:
        print '------------------' + str(search_count) + '-----------------------'
        br.select_form(nr=0)
        end_date = start_date + dt.timedelta(days=1)

        print '    start:', start_date.strftime('%m/%d/%Y')
        print '    end:  ', end_date.strftime('%m/%d/%Y')

        select_items = ['ABSTRACTLSE', 'MEMOLSE', 'OGLSE', 'OGLSEASN']

        cntrls = {'RecordingDateIDStart': start_date.strftime('%m/%d/%Y'),
                  'RecordingDateIDEnd': end_date.strftime('%m/%d/%Y'),
                  '__search_select': select_items}

        for k, v in cntrls.iteritems():
            br.form[k] = v
        req = br.submit()
        html = req.read()
        if test_html(html):
            yield html, start_date, end_date
        br.back()
        start_date = end_date
        search_count += 1
        time.sleep(0.5)

if __name__ == '__main__':
    br = start_browser('url.json')
    start_date = dt.datetime(2006, 1, 1)
    search = search_weld(br, start_date)
    html, start_date, end_date = search.next()
