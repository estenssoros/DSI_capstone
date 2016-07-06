import mechanize
import cookielib
from bs4 import BeautifulSoup
import pandas as pd


br = mechanize.Browser()

cj = cookielib.LWPCookieJar()
br.set_cookiejar(cj)

br.set_handle_equiv(True)
# br.set_handle_gzip(True)
br.set_handle_redirect(True)
br.set_handle_referer(True)
br.set_handle_robots(False)

br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]

cogis_url = 'https://cogcc.state.co.us/cogis/ProductionSearch.asp'

br.open(cogis_url)
# print response.read()
form = br.forms().next()
br.select_form(nr=0)

control_dict = {'grouping_type':['wells'],
                'FromYear':'2016',
                'ToYear':'2016',
                'county':['123'],
                'twp':'1N',
                'rng':'67W',
                'sec':'1',
                'maxrec':['5000']}
for k,v in control_dict.iteritems():
    br.form[k]=v

req = br.submit()
soup = BeautifulSoup(req.read(),'html.parser')
tables = soup.find_all('table')
main_table = tables[1]
trs = main_table.find_all('tr')
