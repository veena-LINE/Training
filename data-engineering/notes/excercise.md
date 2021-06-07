
 touch employees.csv
 
 John,Male
 Mary,Female
 
read the csv using sc.textFile(...)
split the line using comma ,

rdd1 = make a tuple with name and gender (John,Male)

--

touch salary.csv

John,4000
Marry,6000

read the csv using sc.textFile(...)
split the line using comma ,

rdd2 = make a tuple with name and salary (John,4000)

--

then join rdd1 with rdd2

the expected output should be 


(John, (Male, 4000))
(Mary, (Female, 6000))


---


Download the csv

https://raw.githubusercontent.com/midwire/free_zipcode_data/develop/all_us_zipcodes.csv

Load the textFile
Skip the first line

create tuple

sorted
groupByKey
GroupKy



all_zip_codes = sc.textFile("/home/ubuntu/learning/all_us_zipcodes.csv")


all_zip_codes = sc.textFile("/home/ubuntu/learning/all_us_zipcodes.csv")
 r2 = all_zip_codes.filter (lambda line: False if ("code" in line) else True)


r3 = r2.map (lambda line: line.split(","))
r3.take(2)
r4 = r3.map (lambda arr: (arr[0],arr[1],arr[2],arr[3],arr[4],arr[5],arr[6]))

r4.take(2)

r4.takeOrdered(10, key = lambda row: row[2])
  
sorted = r4.sortBy(lambda row: row[2])

r5 = r4.map(lambda row: (row[2], row[:2] + row[3:]))

r6.groupByKey().mapValues(lambda rr: len(rr)).collect()

r6.groupByKey().values().map(lambda c: len(c)).collect()



RDD 

filter and read all cities of specific state
sort function
try group by functions

------

Download this file into local disk
https://raw.githubusercontent.com/midwire/free_zipcode_data/develop/all_us_zipcodes.csv

once downloaded..

1. create a directory in hadoop called "/data"
2. upload all_us_zipcodes.csv from local to hadoop /data directory
3. cat /data/all_us_zipcodes.csv file from hadoop to display the content...

pyspark and read from hadoop
pyspark and write to hadoop
pyspark and write to folder

---

spark-submit





