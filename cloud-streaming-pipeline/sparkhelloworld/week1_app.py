#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A pyspark wordcount program
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("HWE Week 1 App") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')
    df = spark.read.text('../alice-in-wonderland.txt')

    # TODO: pure sql way?
    # df.createOrReplaceTempView('book')
    # query = open('./sparkhelloworld/query.sql').read()
    # spark.sql(query).show()
    
    words = df.select(explode(split(lower(regexp_replace("value", '[^a-zA-Z ]', '')), ' ')).alias('word'))
    counts = words.where(length("word") > 0).groupBy("word").count().orderBy(desc("count"))
    counts.show()