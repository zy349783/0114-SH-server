#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  1 18:15:23 2020

@author: work516
"""


import pandas as pd
def test():   
    df = pd.DataFrame()
    df['stauts'] = ['stored', 'not stored']
    df['mode'] = ['happy', 'sad']
    df['home'] = ['9:30', '10:00']
    print(df)
    df.to_csv('/home/work516/result/test.csv')

if __name__ == '__main__':
    test()