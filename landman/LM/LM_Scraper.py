from __future__ import division
from pymongo import MongoClient
from bs4 import BeautifulSoup
from dateutil import parser
import mechanize
import cookielib
from LM_Utilites import read_json


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

    url = read_json(fname)['login']
    br.open(url)
    br.select_form(nr=1)

    br.form['userId'] = os.environ['WELD_COUNTY_USER_NAME']
    br.form['password'] = os.environ['WELD_COUNTY_PASSWORD']
    br.submit()

    print 'Form submitted! Arrived at {}'.format(br.title())
    return br


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

    for link in br.links():
        if link.text == 'Next':
            print 'switching to:', link.url
            br.follow_link(link)
            results.update(recursive_br(br, k))
            br.back()
            break
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
    base_url = read_json('url.json')['doc_url']
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


def scrape_docs():
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

            write_all_to_s3('.pdf', 'welddocs/')
            clear_docs('.pdf', 'welddocs/')
            write_to_s3('data/new_read.csv')
            print_status(j, i, t_1, t_2)
        time.sleep(60)
    twilio_message('Python script scrape_docs.py done!')
