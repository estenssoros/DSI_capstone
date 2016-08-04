from flask import Flask, request, render_template
import pandas as pd
import requests
from dateutil import parser
import datetime as dt

app = Flask(__name__)

df = pd.read_pickle('../data/new.pickle')
all_data = pd.read_pickle('../data/all_data.pickle')
all_data.set_index('doc', inplace=True)


@app.route('/')
def index():
    return render_template('index.html')



@app.route('/print_text', methods=['POST'])
def print_text():

    township = request.form['township']
    range = request.form['range']
    section = request.form['section']

    trs_str = 'T: {0}, R: {1}, S: {2}'.format(township, range, section)
    trs = '{0}{1}{2}'.format(township, range, section)
    new_df = df[df['TRS'] == trs]
    count = len(new_df)
    content = '''<h1>{0}</h1>
              <br>
              <h2>Found: {1} document(s) matching your search</h2>
              '''.format(trs_str, count)
    if count > 0:
        content += '''<table>
                    <th>select</th>
                    <th>document number</th>
                    <th>recording date</th>
                    <th>lease terms</th>
                    <th>expiration</th>'''

        new_df['rec_date'] = new_df.apply(lambda x: parser.parse(x['rec_date']).date(), axis=1)
        new_df.sort_values('rec_date', ascending=False, inplace=True)
        for i, r in new_df.iterrows():
            doc = r['doc_num']
            years = all_data.loc[doc + '_0', 'years']
            rec_date = r['rec_date']
            exp = rec_date.replace(year=rec_date.year + int(years))
            content += '<tr><td></d><td>{0}</td> <td>{1}</td><td>{2}</td><td>{3}</td></tr>'.format(
                r['doc_num'], rec_date, years, exp)
        content += '</table>'
    return content


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6969, debug=True)
