#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlalchemy as db
import time
import numpy as np
import sys
import pandas as pd
#import tables


def main(connect_string):

    # connection to main DB
    sql_engine = db.create_engine(connect_string)

    # retrieves other parameters: file from previous step;
    # companies table; output files with average values

    infile = input("Input file [fam_by_pat_class.csv]?") or 'fam_by_pat_class.csv'
    tabinname = input("Companies table [patassg]?") or 'patassg'
    outfile1 = input("Output file [avgfam_by_pat.csv]?") or 'avgfam_by_pat.csv'
    outfile2 = input("Output file [avgfam_by_year.csv]?") or 'avgfam_by_year.csv'

    tab_in = config['database'] + '.' + tabinname

    print ('Prog start ', time.ctime())

    # tables read

    sql_in = 'Select distinct a.patent, c.pdpco, c.gvkey, p.appyear From '
    sql_in += 'nber.patassg a Inner Join nber.dynass b On b.pdpass = a.pdpass Inner Join '
    sql_in += 'nber.pdpcohdr c On c.pdpco = b.pdpco1 Inner Join nber.pat76_06_assg p On a.patent = p.patent'

    # dataframe load from previous step
    df_fam= pd.read_csv(infile)  #patent,ipc4,familiarity

    # drop fam 0 values
    df_fam=df_fam[df_fam['familiarity']>0]

    # new dataframe with means calculations
    df_avg_fam=df_fam.groupby('patent').familiarity.mean().reset_index()
    df_avg_fam.rename( columns={"familiarity": "avg_familiarity"}, inplace=True)

    # writes intermediate results to csv file
    with open(outfile1, 'w') as f:
        df_avg_fam.to_csv(f, header=True)
        print('Step 1 completed ', time.ctime())
        f.close

    # step 2

    # reads companies / patent : a.patent, c.pdpco, c.gvkey, p.appyear
    df_pdpco = pd.read_sql_query(sql_in, sql_engine)

    # 2b join company and patent data
    df_pdpco_fam =pd.merge(df_pdpco, df_avg_fam, how='inner', on=['patent'])

    # 2c average by pdpco
    df_fam_pdpco_year = df_pdpco_fam.groupby(['pdpco','appyear']).avg_familiarity.mean().reset_index()

    # writes final results to csv file - by pdpco
    with open('pdpco_'+outfile2, 'w') as f:
        df_fam_pdpco_year.to_csv(f, header=True)
        print('Step 2c completed ', time.ctime())
        f.close
            
    # 2d average by pdpco
    df_fam_gvkey_year = df_pdpco_fam.groupby(['gvkey','appyear']).avg_familiarity.mean().reset_index()

    # writes final results to csv file - by gvkey
    with open('gvkey_'+outfile2, 'w') as f:
        df_fam_gvkey_year.to_csv(f, header=True)
        print('Step 2d completed ', time.ctime())
        f.close
 





# Standard boilerplate to call the main() function.
if __name__ == '__main__':

    # db connection params in config dictionary
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

    print ('Index calc v1.0 201906 Py36')

    # input of root password and default library for data
    config['password'] = input("Mysql password [mysql]?") or 'mysql'
    config['database'] = input("Mysql library [nber]?") or 'nber'

    # connect param: 'mysql+pymysql://USER:PW@DBHOST/DB'
    connect_string = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(config['user'],config['password'],config['host'], config['port'] ,config['database'])

    print (connect_string)

    # call to main routine
    out= main(connect_string)

    print ("end time", time.ctime())
