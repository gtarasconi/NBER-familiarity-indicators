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

    # input of othere params: name of table where data are stored, file to write to
    infile = input("Input file [fam_by_pat_class_comb.csv]?") or 'fam_by_pat_class_comb.csv'
    tabinname = input("Companies table [patassg]?") or 'patassg'
    outfile1 = input("Output file [avg_by_pat.csv]?") or 'avg_by_pat.csv'
    outfile2 = input("Output file [avg_by_year.csv]?") or 'avg_by_year.csv'

    tab_in = config['database'] + '.' + tabinname

    print ('Prog start ', time.ctime())

    # tables read

    sql_in = 'Select distinct a.patent, c.pdpco, c.gvkey, p.appyear From '
    sql_in += 'nber.patassg a Inner Join nber.dynass b On b.pdpass = a.pdpass Inner Join '
    sql_in += 'nber.pdpcohdr c On c.pdpco = b.pdpco1 Inner Join nber.pat76_06_assg p On a.patent = p.patent'

    # tables read in a dataframe df_fam
    df_fam= pd.read_csv(infile)  #,patent,ipc4comb,familiarity,cum_fam

    # drop fam 0 values
    df_fam=df_fam[df_fam['familiarity']>0]

    # new dataframe with means calculations
    # 1a average familiarity by class combination
    df_avg_fam=df_fam.groupby('patent').familiarity.mean().reset_index()
    df_avg_fam.rename( columns={"familiarity": "avg_familiarity"}, inplace=True)

    # output to csv file
    with open('combfam_'+outfile1, 'w') as f:
        df_avg_fam.to_csv(f, header=True)
        print('Step 1a completed ', time.ctime())
        f.close

    # 1b cumulate familiarity by class combination
    df_avg_cumfam=df_fam.groupby('patent').cum_fam.mean().reset_index()
    df_avg_cumfam.rename( columns={"cum_fam": "avg_cum_fam"}, inplace=True)

    # output to csv file
    with open('cumfam_'+outfile1, 'w') as f:
        df_avg_cumfam.to_csv(f, header=True)
        print('Step 1b completed ', time.ctime())
        f.close

    # step 2

    # reads companies / patent : a.patent, c.pdpco, c.gvkey, p.appyear
    df_pdpco = pd.read_sql_query(sql_in, sql_engine)

    # 2b join company and patent data
    df_pdpco_fam =pd.merge(df_pdpco, df_avg_fam, how='inner', on=['patent'])

    # 2c average by pdpco
    df_fam_pdpco_year = df_pdpco_fam.groupby(['pdpco','appyear']).avg_familiarity.mean().reset_index()
    df_fam_pdpco_year['avg_familiarity_sqrt']= df_fam_pdpco_year['avg_familiarity'].apply(np.sqrt)

    # output to csv file
    with open('combfam_pdpco_'+outfile2, 'w') as f:
        df_fam_pdpco_year.to_csv(f, header=True)
        print('Step 2c completed ', time.ctime())
        f.close
            
    # 2d average by pdpco
    df_fam_gvkey_year = df_pdpco_fam.groupby(['gvkey','appyear']).avg_familiarity.mean().reset_index()
    df_fam_gvkey_year['avg_familiarity_sqrt'] = df_fam_gvkey_year['avg_familiarity'].apply(np.sqrt)

    # output to csv file
    with open('combfam_gvkey_'+outfile2, 'w') as f:
        df_fam_gvkey_year.to_csv(f, header=True)
        print('Step 2d completed ', time.ctime())
        f.close

    # 2e join company and patent data
    df_pdpco_cum =pd.merge(df_pdpco, df_avg_cumfam, how='inner', on=['patent'])

    # 2f average cumulate by pdpco
    df_cum_pdpco_year = df_pdpco_cum.groupby(['pdpco', 'appyear']).avg_cum_fam.mean().reset_index()
    df_cum_pdpco_year['avg_cum_fam_sqrt'] = df_cum_pdpco_year['avg_cum_fam'].apply(np.sqrt)

    # output to csv file
    with open('cumfam_pdpco_' + outfile2, 'w') as f:
        df_cum_pdpco_year.to_csv(f, header=True)
        print('Step 2f completed ', time.ctime())
        f.close

    # 2g average cumulate by pdpco
    df_cum_gvkey_year = df_pdpco_cum.groupby(['gvkey', 'appyear']).avg_cum_fam.mean().reset_index()
    df_cum_gvkey_year['avg_cum_fam_sqrt'] = df_cum_gvkey_year['avg_cum_fam'].apply(np.sqrt)

    # output to csv file
    with open('cumfam_gvkey_' + outfile2, 'w') as f:
        df_cum_gvkey_year.to_csv(f, header=True)
        print('Step 2g completed ', time.ctime())
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
    print ('B02 Index calc v1.0 201905 Py36')
    config['password'] = input("Mysql password [mysql]?") or 'mysql'
    config['database'] = input("Mysql library [nber]?") or 'nber'

    # connect param: 'mysql+pymysql://USER:PW@DBHOST/DB'
    connect_string = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(config['user'],config['password'],config['host'], config['port'] ,config['database'])

    print (connect_string)

    # call of main procedure
    out= main(connect_string)

    print ("end time", time.ctime())
