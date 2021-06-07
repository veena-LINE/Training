```sql
create external schema spectrum
from data catalog 
database 'orderdb' iam_role 'arn:aws:iam::854633459381:role/GK_REDSHIFT_S3'
create external database if not exists;
```
--
-- we are creating table in data lake formation called sales, in db orderdb
-- sprectrum schema is linked to orderdb

```
create external table spectrum.sales(
 id integer,
 qty integer,
 amount float)
row format delimited
 fields terminated by ','
 stored as textfile
 location 's3://gk-aws-workshop/sales'
 table properties ('numRows'='172000');
```

```
select * from spectrum.orders;
select * from spectrum.sales;
```
