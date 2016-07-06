import mechanize
import cookielib
from bs4 import BeautifulSoup
import pandas as pd
import os
import datetime as dt

url = 'https://searchicris.co.weld.co.us/recorder/web/login.jsp?submit=I+Acknowledge'

br = mechanize.Browser(factory=mechanize.RobustFactory())
br.open(url)
br.select_form(nr=1)
# for form in br.forms():
#     print form

br.form['userId'] = os.environ['WELD_COUNTY_USER_NAME']
br.form['password'] = os.environ['WELD_COUNTY_PASSWORD']
br.submit()

print 'Form submitted! Arrived at {}'.format(br.title())

br.select_form(nr=0)

start_date = dt.datetime(2006, 1, 1)
# end_date = start_date.replace(year = start_date.year + 1)
end_date = start_date + dt.timedelta(days=1)

select_items = ['ABSTRACTLSE', 'MEMOLSE', 'OGLSE', 'OGLSEASN']
cntrls = {'RecordingDateIDStart': start_date.strftime('%d/%m/%Y'),
          'RecordingDateIDEnd': end_date.strftime('%d/%m/%Y'),
          '__search_select': select_items}

for k, v in cntrls.iteritems():
    br.form[k] = v
req = br.submit()
print 'success!'
