!conda install pyspark
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import os
import sys
import requests
import datetime

# def aggregate_tags_count(new_values, total_sum):
#     return sum(new_values) + (total_sum or 0)

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
        row_rdd = rdd.map(lambda w: Row(a_time=w[0], b_irradiance = 0 if (round(-5649.96 + 60.16*float(w[1]) + 15.05*float(w[2]) + 4.24*float(w[3]) + 
                               0.74*float(w[4]) + 4.11*float(w[5]) + 77.77*float(w[6]) - 87.33*float(w[7]) + 
                               50.11*float(w[8]) + 63.83*float(w[9]),2)<0) else round(-5649.96 + 60.16*float(w[1]) + 15.05*float(w[2]) + 4.24*float(w[3]) + 
                               0.74*float(w[4]) + 4.11*float(w[5]) + 77.77*float(w[6]) - 87.33*float(w[7]) + 
                               50.11*float(w[8]) + 63.83*float(w[9]),2) ))
        # create a DF from the Row RDD
        weather_df = sql_context.createDataFrame(row_rdd)
        weather_df.write.csv("output", mode='append', header=True)

        # Register the dataframe as table
        # weather_df.registerTempTable("weather2watt")
        # # get the top 10 hashtags from the table using SQL and print them
        # weather_power_df = sql_context.sql(
        #     "select * from weather2watt")

        weather_df.show()
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# create spark configuration
conf = SparkConf()
conf.setAppName("Weather2WattStreamApp")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 20 seconds
ssc = StreamingContext(sc, 20)

# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpointwindow")

# read data from the port
weather = ssc.socketTextStream("localhost", 9091)

# split each weather average from input string
weather_avg = weather.map(lambda line: line.decode().split(","))

# filter the values to get the count of hours in the week
# weather_avgs = weather_avg.map(lambda x: (x, 1))

# adding the count of each irradiance value to its last hour count using updateStateByKey
# weather_totals = weather_avgs.updateStateByKey(aggregate_tags_count)
# weather_totals.pprint()

# do the processing for each RDD generated in each interval
weather_avg.foreachRDD(process_rdd)

# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()
