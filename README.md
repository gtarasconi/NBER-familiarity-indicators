# NBER-familiarity-indicators
Set of MySQL and python scripts to create familiarity indicator by IPC class

The set of scripts allow to create patents familiarity by IPC4 (set A) or IPC4 combination (SET B)

Prerequisites:

NBER data
source https://sites.google.com/site/patentdataproject/Home



Patentsview applications table (for applciation date)
http://www.patentsview.org/download/  table application

Python version used: 3.5 or above
Modules needed:
Pandas, sqlalchemy, pymysql:




programs are divided into 3 batches to run strictly in progression by prefix (i.e. A01, A02... )

A builds familiarity by class

B builds familiarity by class comination

C builds DISTANT citation based indicator


A01: SQL script - creates in MySQL the environment and FAMt0 table that is the source for Python programs following.

A02  Python script - creates familiarity by patent / IPC4 - intermediate result

A03  Python script - creates familiarity by company / year - final result



B01  Python script - creates familiarity by patent / IPC4 combination - intermediate result

B02  Python script - creates familiarity by company / year - final result
