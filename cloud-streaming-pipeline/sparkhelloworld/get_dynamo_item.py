import boto3

my_session = boto3.Session(profile_name='hwe_prod')

# Create a DynamoDB client
dynamodb = my_session.client('dynamodb')

table_name = 'customers'
key = {
    'customer_id': {'S': '10000023'} 
}
response = dynamodb.get_item(TableName=table_name,Key=key)
item = response['Item']
print(item)