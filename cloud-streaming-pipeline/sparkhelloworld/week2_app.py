#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
An example Pyspark Structured Streaming app that reads data from Kafka

Run using:
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
sparkhelloworld/week2_app.py
"""
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
from pyspark.sql.functions import *

def getScramAuthString(username, password):
    scram = 'org.apache.kafka.common.security.scram.ScramLoginModule required\n'
    scram += f'username="{username}"\n'
    scram += f'password="{password}";'
    return scram

def main():
    config = load_dotenv(".env")
    if not config:
        raise Exception("Unable to load config")
    
    username = os.environ['HWE_USERNAME']
    password = os.environ['HWE_PASSWORD']
    bootstrap_servers = os.environ['HWE_BOOTSTRAP']
    print("Connecting to kafka servers %s" % (bootstrap_servers))

    spark = SparkSession \
        .builder \
        .appName("Week 2 App") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    # Create DataFrame with (key, value)
    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', bootstrap_servers) \
        .option('subscribe', 'word-count') \
        .option('maxOffsetsPerTrigger', 10) \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.ssl.truststore.location", 'kafka.client.truststore.jks') \
        .option("kafka.sasl.jaas.config", getScramAuthString(username, password)) \
        .load() \
        .selectExpr('CAST(value AS STRING)')
    
    words = df.select(explode(split(lower(regexp_replace("value", '[^a-zA-Z ]', '')), ' ')).alias('word'))
    counts = words.where(length("word") > 0).groupBy("word").count().orderBy(desc("count"))
    # counts.show()
    
    # Start running the query that prints the running counts to the console
            # 

    stream = counts.writeStream \
        .format("console") \
        .outputMode("complete") \
        .start()

    stream.awaitTermination()

if __name__ == "__main__":
    main()
