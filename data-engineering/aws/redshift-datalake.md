```
movies.csv
1,Good Bad Ugly,1999
```

```
rating.csv [id, rating, user_id ]
1,5,100
1,4,200
1,3,300
```
-----------

In data lake formation

- create a db called moviedb 
- create a table called movies in moviedb, with schema, (id int, name string,year int)
- create a s3 folder called s://..../movies
- upload movies.csv there
- 
----------------------------------

in your redshift, 

- create an external schema called moviedb, refers to moviedb in catalog
- create an external table called ratings with columns (id, rating, user_id), refers to moviedb schema [indirectly into moviedb catalog]
    with s3 path, s://..../ratings

- upload the ratings.csv into s3 bucket

 
```
select * from moviedb.movies
select * from moviedb.ratings
```

create a redshift schema [not external], called "account"
    within account schema, create table called "users" with (id int, gender text)

    insert data into account.users ()..


join between them.
