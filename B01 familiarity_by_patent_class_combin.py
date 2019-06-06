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
    outfile = input("Output file [fam_by_pat_class_comb.csv]?") or 'fam_by_pat_class_comb.csv'

    tab_in = config['database'] + '.' + tabinname

    print ('Prog start ', time.ctime())

    # tables read

    sql_in = 'select * from ' + tab_in

    # tables read in a dataframe df_ipc
    # explicit sort by class ,date
    df_ipc = pd.read_sql_query(sql_in, sql_engine).sort_values(by=['ipc4','appln_date'])
    print ('Data read ', time.ctime())

    # drops useless columns
    df_ipc.drop(columns=['daydiff', 'yeardiff'], inplace=True)

    # work on familiarity

    # step 1 creates class combinations

    df_ipc['ipc4comb'] = df_ipc.groupby(['patent'])['ipc4'].transform('sum') #apply(lambda x: "{%s}" % ', '.join(x))

    # step 2 drops ipc4, makes distinct and resorts
    df_ipc=df_ipc.drop(columns=['ipc4']).drop_duplicates().sort_values(by=['ipc4comb', 'appln_date'])

    # step3 cumulative sum of familiarity
    df_ipc['csum'] = df_ipc.groupby(['ipc4comb'])['familiarity_raw'].cumsum()

    # correction where date is the same take min
    df_ipc['familiarity0'] = df_ipc.groupby(['ipc4comb', 'appln_date'])['csum'].transform('min')

    # calculate final value divide by raw fam and -1
    df_ipc['familiarity']=df_ipc['familiarity0']/df_ipc['familiarity_raw']-1

    df_ipc['dummy'] = 1
    df_ipc['cum_fam0'] = df_ipc.groupby(['ipc4comb'])['dummy'].cumsum()

    # correction where date is the same take min
    df_ipc['cum_fam'] = df_ipc.groupby(['ipc4comb', 'appln_date'])['cum_fam0'].transform('min')

    # dont count myself as predecessor
    df_ipc['cum_fam']=df_ipc['cum_fam']-1

    df_fam=df_ipc[['patent','ipc4comb','familiarity', 'cum_fam']]

    # output to csv file for next step
    with open(outfile, 'w') as f:
        df_fam.to_csv(f, header=True)
        print('Output written ', time.ctime())
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
    print ('B01 Index calc v1.0 201905 Py36')
    config['password'] = input("Mysql password [mysql]?") or 'mysql'
    config['database'] = input("Mysql library [nber]?") or 'nber'

    # connect param: 'mysql+pymysql://USER:PW@DBHOST/DB'
    connect_string = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(config['user'],config['password'],config['host'], config['port'] ,config['database'])

    print (connect_string)

    # call of main procedure
    out= main(connect_string)

    print ("end time", time.ctime())
