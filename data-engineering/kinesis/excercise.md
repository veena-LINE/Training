1. Clone consumer_client.py, producer.py files
2. 
3. Generate invoice data with producer, publish to Kinesis Stream
     the below fields should be there in produced data

```
InvoiceNo
StockCode
Description
Quantity
InvoiceDate
UnitPrice
CustomerID
Country


{
    "InvoiceNo" : "536365",
    ...
    "Quantity" : random between 1 to 10,
    "Price": randin between 1 to 100
    ..
}

producer the json record based on above structure

536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,12/1/2010 8:26,2.55,17850,United Kingdom
536365,71053,WHITE METAL LANTERN,6,12/1/2010 8:26,3.39,17850,United Kingdom
```

4. Consumer invoice data using consumer, get the record data, do a little operation
```
    get the record first
       print Quantity * Price
```
