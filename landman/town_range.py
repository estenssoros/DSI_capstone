import numpy as np
import pandas as pd
from pymongo import MongoClient
from bs4 import BeautifulSoup
import urllib
import requests
import os

def generate_coord(t_lst, r_lst):
    options = []
    for t in t_lst:
        for r in r_lst:
            # if len(t)==1:
            #     t = '0'+ t
            if int(r)==68 and int(t) > 4:
                pass
            elif int(r) in [60,59,58,57,56] and int(t) < 7:
                pass
            else:
                coord = (t + 'N', r + 'W')
                options.append(coord)
    return options

def permute_df(df,column,lst):
    df[column] = lst.pop(0)
    new_df = df.copy()
    for i in lst:
        new_df[column] = i
        df = df.append(new_df,ignore_index=True)
    return df

def create_df():
    r_lst = [str(x) for x in list(range(56,68))]
    t_lst = [str(x) for x in list(range(1,13))]
    options_tup = generate_coord(t_lst, r_lst)
    options_str = [''.join(x) for x in options_tup]

    df = pd.DataFrame(options_tup,columns=['t_ship','range'])
    df = permute_df(df,'section',range(1,37))
    df = permute_df(df,'year',range(2000,2017))
    return df


if __name__=='__main__':
    pass
