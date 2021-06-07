
word_count = sc.textFile('/home/ubuntu/book.txt') \
  .map (lambda x: x.strip()) \
  .filter (lambda x: len(x) > 0) \
  .flatMap(lambda line: line.split(" ")) \
  .reduceByKey(lambda acc, value: acc + value)

word_count.saveAsTextFile("/home/ubuntu/word-count-result2")
