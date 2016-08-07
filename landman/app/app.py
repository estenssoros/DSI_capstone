from flask import Flask, request, render_template, make_response, send_file
import pandas as pd
import requests
from dateutil import parser
import datetime as dt
from util import get_docs_from_s3, clear_docs
import os
import zipfile

app = Flask(__name__)

df = pd.read_pickle('../data/new.pickle')
df['doc_num'] = df['doc_num'] + '_0'
all_data = pd.read_pickle('../data/all_data.pickle')
df = pd.merge(df[['doc_num', 'rec_date', 'TRS']], all_data[['doc', 'years']], how='left', left_on='doc_num', right_on='doc')
del df['doc']


def add_years(d, years):
    try:
        d.replace(year=d.year + int(years))

    except:
        pass
    return d


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/results', methods=['POST'])
def results():

    township = request.form['township']
    range = request.form['range']
    section = request.form['section']

    trs = '{0}{1}{2}'.format(township, range, section)
    new_df = df[df['TRS'] == trs]
    count = len(new_df)
    if count > 0:
        new_df['rec_date'] = new_df.apply(lambda x: parser.parse(x['rec_date']).date(), axis=1)
        new_df.sort_values('rec_date', ascending=False, inplace=True)
        new_df['exp'] = new_df.apply(lambda x: add_years(x['rec_date'], x['years']), axis=1)
        data = [tuple(x) for x in new_df[['doc_num', 'rec_date', 'years', 'exp']].values]
    else:
        data = [()]

    return render_template('results.html', trs=trs, count=count, data=data)


@app.route('/download_docs', methods=['POST'])
def download_docs():
    clear_docs(ext=['.zip','.pdf'])

    docs = [doc[:-1] for doc in request.form]

    if len(docs) == 0:
        return 'No docs selected!'

    directory = 'welddocs/'
    ext = '.pdf'

    if not os.path.exists(directory):
        os.mkdir(directory)

    get_docs_from_s3(docs)

    docs = [d for d in os.listdir(directory) if d.endswith(ext)]
    if len(docs) > 1:
        timestamp = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
        zfname = 'docs-' + str(timestamp) + '.zip'
        with zipfile.ZipFile(directory + zfname, 'a') as zf:
            for doc in docs:
                zf.write(directory + doc, doc)
        target = zf.filename
        clear_docs()
    else:
        target = directory + docs[0]
    return send_file(target, as_attachment=True)
    # response = make_response()
    # response.headers['Cache-Control'] = 'no-cache'
    # response.headers['Content-Type'] = 'application/zip'
    # response.headers['X-Accel-Redirect'] = '/welddocs/' + zf.filename

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6969, debug=True)
