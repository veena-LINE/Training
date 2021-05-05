echo "product_code,quantity" >> stocks.csv
echo "10000,1000" >> stocks.csv
echo "10010,100" >> stocks.csv
echo "12234,345" >> stocks.csv
echo "12345,12378" >> stocks.csv


hdfs dfs -mkdir /inventory
hdfs dfs -put ./stocks.csv /inventory
hdfs dfs -get /inventory/stocks.csv inventory_get.csv
hdfs dfs -rm /inventory/stocks.csv
hdfs dfs -ls /inventory
