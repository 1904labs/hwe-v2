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
import boto3
from collections import namedtuple

Person = namedtuple("Person", ["customer_id", "username", "name", "email", "birthdate"])

def getScramAuthString(username, password):
    scram = 'org.apache.kafka.common.security.scram.ScramLoginModule required\n'
    scram += f'username="{username}"\n'
    scram += f'password="{password}";'
    return scram

def get_dynamo_key(id):
    return { 'customer_id': {'S': id} }

def dynamo_item_to_tuple(item):
    customer_id = item['customer_id']['S']
    return { customer_id : Person(item['customer_id']['S'], item['username']['S'], item['name']['S'], item['email']['S'], item['birthdate']['S']) }

def get_customer_ids(partition):
    return [row.customer_id for row in partition]

def get_customers_from_dynamo(customer_ids):
    # boto3 session, profile name must match a profile the aws creds
    my_session = boto3.Session(profile_name='hwe_prod')
    # Create a DynamoDB client
    dynamodb = my_session.client('dynamodb')
    customer_id_keys = [get_dynamo_key(cid) for cid in customer_ids]
    request_items = {
        'customers': {
            'Keys': customer_id_keys
        }
    }
    return dynamodb.batch_get_item(RequestItems=request_items)

def partition_mapper(partition):
    # first, get all of the customer IDs from each review
    customer_ids = get_customer_ids(partition)
    # second, using those IDs, retrieve each customer record from dynamo
    dynamo_result = get_customers_from_dynamo(customer_ids)
    customers = { dynamo_item_to_tuple(item) for item in dynamo_result['Responses']['customers'] }
    for row in partition:
        if row.customer_id in customers:
            customer = customers[row.customer_id]
            username = customer.username
            email = customer.email
        else:
            username = ""
            email = ""
        yield [row.review_id, row.product_title, row.customer_id, username, email]

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
    schema = 'marketplace STRING, customer_id STRING, review_id STRING, product_id STRING, product_parent STRING, product_title STRING, product_category STRING, star_rating STRING, helpful_votes STRING, total_votes STRING, vine STRING, verified_purchase STRING, review_headline STRING, review_body STRING, review_date STRING'
    reviews_df = df.select(from_csv('value', schema, { 'delimiter': '\t' }).alias('csv')).select('csv.*')
   
    result = reviews_df.mapPartitions(partition_mapper).toDF(["review_id", "product_title", "customer_id", "username", "email"])

    stream = result.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    stream.awaitTermination()

if __name__ == "__main__":
    main()
