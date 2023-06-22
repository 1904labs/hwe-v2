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
from pyspark.sql.types import *

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
        .appName("Week 6 App") \
        .getOrCreate()

    # read in the customer data from csv
    customer_csv_schema = StructType([
        StructField('customer_id', StringType(), True),
        StructField('username', StringType(), True),
        StructField('name', StringType(), True),
        StructField('sex', StringType(), True),
        StructField('email', StringType(), True),
        StructField('birthdate', StringType(), True)
    ])
    spark.sparkContext.setLogLevel('WARN')
    customer_df = spark.read \
        .option('delimiter', '\t') \
        .option('header', True) \
        .option('schema', customer_csv_schema) \
        .csv('customer_ids_hbase.txt')

    # Create DataFrame with (key, value)
    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', bootstrap_servers) \
        .option('subscribe', 'reviews') \
        .option('maxOffsetsPerTrigger', 10) \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.ssl.truststore.location", 'kafka.client.truststore.jks') \
        .option("kafka.sasl.jaas.config", getScramAuthString(username, password)) \
        .load() \
        .selectExpr('CAST(value AS STRING)')
    
    # parse out the columns from the incoming stream of tab-separated data
    # note: from_csv returns a struct, thus we have some sillyness to get all the columns
    reviews_csv_schema = 'marketplace STRING, customer_id STRING, review_id STRING, product_id STRING, product_parent STRING, product_title STRING, product_category STRING, star_rating STRING, helpful_votes STRING, total_votes STRING, vine STRING, verified_purchase STRING, review_headline STRING, review_body STRING, review_date STRING'
    reviews_df = df.select(from_csv('value', reviews_csv_schema, { 'delimiter': '\t' }).alias('csv')).select('csv.*')
    result = reviews_df.join(customer_df, 'customer_id')

    stream = result.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    stream.awaitTermination()

if __name__ == "__main__":
    main()