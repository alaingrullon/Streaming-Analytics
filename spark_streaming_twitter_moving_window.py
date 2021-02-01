from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtags=w[0], hashtags_count=w[1]))
        # create a DF from the Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        hashtags_df.registerTempTable("hashtags")
        # get the top 10 hashtags from the table using SQL and print them
        hashtags_counts_df = sql_context.sql(
            "select hashtags, hashtags_count from hashtags order by hashtags_count desc limit 10")
        hashtags_counts_df.show()
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 30 seconds
ssc = StreamingContext(sc, 30)

# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpointwindow")

# read data from the port
tweet = ssc.socketTextStream("localhost", 9007)

# split each tweet into words
words = tweet.flatMap(lambda tweet: tweet.split(" "))

# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtags,1)
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))

# adding the count of each hashtag to its last count using updateStateByKey
tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
tags_totals.pprint()

# do the processing for each RDD generated in each interval
tags_totals.foreachRDD(process_rdd)

# Top 10 words using a moving window of 10 minutes every 30 seconds
top10words = words.map(lambda word: (word, 1))
wordCounts = top10words.reduceByKeyAndWindow(lambda x: int(x), 600, 30)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()