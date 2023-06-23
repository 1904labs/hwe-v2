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
# import boto3
from collections import namedtuple
import pandas as pd
from pyspark.sql.types import *

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

def get_customer_ids(reviews_df):
    return [c.customer_id for c in reviews_df.select('customer_id').collect()]

def get_customer_ids(reviews_df):
    return [c.customer_id for c in reviews_df.select('customer_id').collect()]

def get_customers_from_dynamo(dynamodb, customer_ids):
    customer_id_keys = [get_dynamo_key(cid) for cid in customer_ids]
    request_items = {
        'customers': {
            'Keys': customer_id_keys
        }
    }
    return dynamodb.batch_get_item(RequestItems=request_items)

# def dynamo_response_to_dataframe(spark, dynamo_result):
#     data = [dynamo_item_to_tuple(item) for item in dynamo_result['Responses']['customers']]
#     return spark.createDataFrame(data, ['customer_id', 'username', 'name', 'email', 'birthdate'])

# def foreach_batch_func(reviews_df, batch_id):
#     print(f">>>>>>>>>>>>>>> processing {batch_id}")
#     # first, get all of the customer IDs from each review
#     customer_ids = get_customer_ids(reviews_df)
#     # second, using those IDs, retrieve each customer record from dynamo
#     dynamo_result = get_customers_from_dynamo(customer_ids)

#     # finally, augment each review 
#     spark = reviews_df._session
#     customers_df = dynamo_response_to_dataframe(spark, dynamo_result)
#     joined_df = reviews_df.join(customers_df, 'customer_id')
#     result = joined_df.select('customer_id', 'review_id', 'product_title', 'username', 'name')
#     result.foreach(print)

def map_partition(pandas_df_iterator, dynamodb):
    print(f">>>>>>>>>>>>>>> map_partition")
    
    for pandas_df in pandas_df_iterator:
        print('pandas_df:', pandas_df)
        # first, get all of the customer IDs from each review
        customer_ids = pandas_df['customer_id'].tolist()
        print('customer_ids:', customer_ids)
        
        print('dynamodb')
        # second, using those IDs, retrieve each customer record from dynamo
        dynamo_result = get_customers_from_dynamo(dynamodb, customer_ids)
        customers = { dynamo_item_to_tuple(item) for item in dynamo_result['Responses']['customers'] }
        print('customers:', customers)
        customers_pandas_df = pd.DataFrame(customers)
        # finally, augment each review with customer data
        joined_pandas_df = pandas_df.join(customers_pandas_df, 'customer_id')
        yield joined_pandas_df

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

    #customer_id	username	name	sex	email	birthdate
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
    
    # Doesn't support dropping to rdd
    # result = reviews_df.rdd.mapPartitions(map_partition).toDF(['product_title', 'customer_id', 'username'])
    
    # def wrapper(dfs):
    #     # boto3 session, profile name must match a profile the aws creds
    #     my_session = boto3.Session(profile_name='hwe_prod')
    #     print('session')
    #     # Create a DynamoDB client
    #     dynamodb = my_session.client('dynamodb')
    #     print('dynamodb')
    #     return map_partition(dfs, dynamodb)
    
    # result = reviews_df.mapInPandas(wrapper, schema=reviews_csv_schema + ', username STRING, name STRING, email STRING, birthdate STRING')
    result = reviews_df.join(customer_df, 'customer_id')

    stream = result.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    stream.awaitTermination()

if __name__ == "__main__":
    main()