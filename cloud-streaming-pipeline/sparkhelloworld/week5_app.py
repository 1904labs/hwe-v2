#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A pyspark program which enriches a dataset from DynamoDB
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import boto3
from collections import namedtuple

#Use a namedtuple as a class to represent the data coming back from DynamoDB
#(Like a Scala case class)
Person = namedtuple("Person", ["name", "birthdate", "customer_id"])

#Fake input data, this will come from Kafka in reality
def build_fake_input_data(num_records, num_partitions, spark):
    keys = get_random_customer_ids(num_records)
    # Define the schema for the DataFrame
    # Note: I only want this to have 1 field: customer_id. But I get an error?
    # If I add a 2nd bogus field everything works fine?
    schema = StructType([
        StructField("customer_id", StringType(), nullable=False),
        StructField("bogus", StringType(), nullable=False)
    ])
    keys_with_bogus_data = [(key, "bogus") for key in keys]
    return spark.createDataFrame(data=keys_with_bogus_data, schema=schema).repartition(num_partitions)

#Get random customer IDs from DynamoDB customers table
def get_random_customer_ids(num):
    my_session = boto3.Session(profile_name='hwe_prod')
    dynamodb = my_session.client('dynamodb')
    table_name = 'customers'
    partition_key = 'customer_id'
    # Scan the table for random values of the partition key
    scan_params = {
    'TableName': table_name,
    'Limit': num
    }
    response = dynamodb.scan(**scan_params)
    
    items = response['Items']
    random_partition_keys = [item[partition_key]['S'] for item in items]
    return random_partition_keys

#Query DynamoDB for a given list of customer IDs (representing a Spark partition)
#This function is meant to be passed into a mapPartitions function in Spark.
def query_dynamo_for_a_partition(partition):
    # Create a DynamoDB client
    my_session = boto3.Session(profile_name='hwe_prod')
    dynamodb = my_session.client('dynamodb')
    table_name = 'customers'
    people = []
    for item in partition:
        key = {
        'customer_id': {'S': item.customer_id} 
        }
        response = dynamodb.get_item(TableName=table_name,Key=key)
        item = response['Item']
        person = Person(item['name']['S'], item['birthdate']['S'], item['customer_id']['S'])
        people.append(person)
    return people


 ##   ##   ##   # #    # 
 # # # #  #  #  # ##   # 
 #  #  # #    # # # #  # 
 #     # ###### # #  # # 
 #     # #    # # #   ## 
 #     # #    # # #    # 
def main():
    spark = SparkSession \
        .builder \
        .appName("HWE Week 5 App") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel('WARN')

    #This will be coming from Kafka in reality...
    fake_input_data = build_fake_input_data(num_records=500,  spark=spark, num_partitions=10)
    
    #Enrich from DynamoDB using mapPartitions
    people = fake_input_data.rdd.mapPartitions(lambda partition: query_dynamo_for_a_partition(partition)).toDF(["name", "birthdate", "customer_id"])
    print(people.collect())


if __name__ == "__main__":
    main()