import pandas as pd
import numpy as np
from dateutil import parser
import matplotlib.pyplot as plt
plt.style.use('ggplot')

# apply function
def apply_to_col(df,cols,func):
    for col in cols:
        df[col] = df.apply(lambda x: func(x[col]),axis=1)
    return df

# date_time columns
def get_date(s):
    try:
        return parser.parse(s)
    except:
        return np.nan
# convert $$ to float
def to_float(s):
    return float(s.replace('$','').replace(',',''))

# remame columns function
def fix_col(s):
    s = s.replace('.','').replace(' ','_').lower()
    return s

def load_data():
    df = pd.read_csv('data/LOAD_SUMMARY-Enerplus_Hall_4_well.csv',header = None)

    for i, r in df.iterrows():
        if r.isnull().sum() == 0:
            start_index = i
            break

    info = df.iloc[range(4)]
    info = info[[0,1]]

    df.columns = df.iloc[start_index]
    for i in range(start_index+1):
        df = df.drop([i])

    for i, r in info.iterrows():
        col = r[0]
        data = r[1]
        df[col] = data

    cols = ['Accepted','Loader','In Transit','At Dest','Delivered','Ordered','Assigned','Report Generate']
    df = apply_to_col(df,cols,get_date)

    cols = ['Dem. At Loader','Dem. At Dest','Total Hauling']
    df = apply_to_col(df,cols,to_float)

    columns = [fix_col(x) for x in df.columns.tolist()]
    df.columns = columns

    job_name = df.iloc[0]['job_name']
    order_num = df.iloc[0]['order_#']
    report_gen = str(df.iloc[0]['report_generate'].date())
    fname = '_'.join([job_name,order_num,report_gen])

    df.to_pickle('data/'+'d1'+'.pickle')


if __name__ == '__main__':
    # load_data()
    df = pd.read_pickle('data/d1.pickle')
    df =df.set_index('delivered')
    product = set(df['product'])
    for p in product:
        p_df = df[df['product']==p]
        p_df = p_df['total_hauling'].resample('H').sum()
        p_df.hist(label=p)
    plt.legend()
    plt.show()
