

-- AA01 - preliminary tables
-- uses a schema named NBER where data from NBER have been downloaded and installed in MySQL
-- source https://sites.google.com/site/patentdataproject/Home

-- application dates are not included so we get it from patentsview
-- http://www.patentsview.org/download/  table application


use nber;


-- step 1: creates link to appln dates - pwappdates

-- pwapps : turns patent id into numeric and keeps app date only

drop table if exists pwapps;

create table pwapps
Select distinct
    cast(a.patent_id as unsigned) patent_id,
    a.date As appln_date
From
    patentsview.application a
where patent_id not regexp('[A-Z]');

alter table pwapps add index i1(patent_id);
alter table pat76_06_assg add index t1(patent);


drop table if exists pwappdates;

create table pwappdates
Select
    a.patent,
    b.appln_date
From
    pat76_06_assg a Left Join
    pwapps b On b.patent_id = a.patent;
	
alter table pat76_06_assg drop index t1;

alter table pwappdates add index i1(patent);

-- step 2 work table 


alter table pat76_06_ipc add index t1(patent);

-- creates table with familiarity computed from T0
-- in python we use that as baseline for calculation

drop table if exists famT0;

set @T0 = date('2019-12-31');

create table famT0
Select distinct
    a.patent,
    b.appln_date,
    ucase(a.icl_class) as ipc4,
    datediff(@T0 , b.appln_date) as daydiff,
    4*(datediff(@T0 , b.appln_date))/(365*4+1) as yeardiff,
    exp(-1*(4*(datediff(@T0 , b.appln_date))/(365*4+1))/5) as familiarity_raw
From
    pwappdates b Inner Join
    pat76_06_ipc a On b.patent = a.patent
 WHERE year(b.`appln_date`) > 1963 and year(b.`appln_date`) <2007;


ALTER TABLE `famt0`  ADD INDEX `I_1`(`ipc4`);
ALTER TABLE `famt0`  ADD INDEX `I_2`(`appln_date`);





