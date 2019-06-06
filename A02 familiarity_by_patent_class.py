#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlalchemy as db
import time
import numpy as np
import sys
import pandas as pd
#import tables


def main(connect_string):

    # connection to database
    sql_engine = db.create_engine(connect_string)

    pd.options.mode.chained_assignment = None

    # input of othere params: name of table where data are stored, file to write to
    tabinname = input("Input table [famt0]?") or 'famt0'
    outfile = input("Output file [fam_by_pat_class.csv]?") or 'fam_by_pat_class.csv'

    tab_in = config['database'] + '.' + tabinname

    print ('Prog start ', time.ctime())

    # tables read in a dataframe df_ipc

    sql_in = 'select * from ' + tab_in
    # explicit sort by class ,date
    df_ipc = pd.read_sql_query(sql_in, sql_engine).sort_values(by=['ipc4','appln_date'])

    # drops useless columns
    df_ipc.drop(columns=['daydiff', 'yeardiff'], inplace=True)

    # work on familiarity
    # step 1 cumulate group by class

    df_ipc['csum'] = df_ipc.groupby(['ipc4'])['familiarity_raw'].cumsum()

    # correction where date is the same take min
    df_ipc['familiarity0'] = df_ipc.groupby(['ipc4', 'appln_date'])['csum'].transform('min')

    # calculate final value divide by raw fam and -1
    df_ipc['familiarity']=df_ipc['familiarity0']/df_ipc['familiarity_raw']-1
    df_fam=df_ipc[['patent','ipc4','familiarity']]

    # output to csv file for next step
    with open(outfile, 'w') as f:
        df_fam.to_csv(f, header=True)
        f.close
            




# Standard boilerplate to call the main() function.
if __name__ == '__main__':

    # config dictionary contains connection parameters
    config = {
        'host': 'localhost',
        'port': '3306',
        'database': 'test',
        'user': 'root',
        'password': ' ',
        'charset': 'utf8',
        'use_unicode': True,
        'get_warnings': True,
    }

    # input of default password for root and library
    print ('Index calc v1.0 201905 Py36')
    config['password'] = input("Mysql password [mysql]?") or 'mysql'
    config['database'] = input("Mysql library [nber]?") or 'nber'

    # connect param: 'mysql+pymysql://USER:PW@DBHOST/DB'
    connect_string = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(config['user'],config['password'],config['host'], config['port'] ,config['database'])

    print (connect_string)

    # call of main procedure
    out= main(connect_string)

    print ("end time", time.ctime())
