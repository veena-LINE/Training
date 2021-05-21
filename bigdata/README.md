**echo** "product_code,quantity" **>** stocks.csv
<br>**echo** "10000,1000" **>>** stocks.csv
<br>**echo** "10010,100" **>>** stocks.csv
<br>**echo** "12234,345" **>>** stocks.csv
<br>**echo** "12345,12378" **>>** stocks.csv

<br>**hdfs** **dfs** **-mkdir** /inventory
<br>**hdfs** **dfs** **-put** ./stocks.csv /inventory
<br>**hdfs** **dfs** **-get** /inventory/stocks.csv inventory_get.csv
<br>**hdfs** **dfs** **-r**m /inventory/stocks.csv
<br>**hdfs** **dfs** **-ls** /inventory
