from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("first_challenge").getOrCreate()
sc = spark.sparkContext

rdd = spark.sparkContext.textFile("wordcount.txt")
word_count = rdd.flatMap(lambda word: word.split(" ")).map(lambda word: (word.lower(),1) if len(word) < 11 
                                                           else ('MAIORES QUE 10',1))

grouped_data = word_count.reduceByKey(lambda a,b:a +b)

df = spark.createDataFrame(grouped_data).selectExpr('_1 as word','_2 as count_word')
df.coalesce(1).write.mode("overwrite").option("header","true").csv("wordcount")
