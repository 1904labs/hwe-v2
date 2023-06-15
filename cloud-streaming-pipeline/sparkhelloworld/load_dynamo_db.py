# HBASE_ROW_KEY,f1:username,f1:name,f1:sex,f1:mail,f1:birthdate
# aws sso login --profile hwe_prod

import awswrangler as wr
import boto3
import pandas as pd

# Read CSV file into a DataFrame
schema = {
    'customer_id': str,
    'username': str,
    'name': str,
    'sex': str,
    'email': str,
    'birthdate': str
}
df = pd.read_csv('./customer_ids_hbase.txt', delimiter='\t',dtype=schema)
print(df.head(10))
print(df.dtypes)

my_session = boto3.Session(profile_name='hwe_prod')
boto3.setup_default_session(my_session)
#https://aws-sdk-pandas.readthedocs.io/en/stable/stubs/awswrangler.dynamodb.put_df.html#awswrangler.dynamodb.put_df
# had to turn use_threads to False or else got KeyError: 'credential_provider'
# this will go super slow to load 2.5 million records, probably an hour?
wr.dynamodb.put_df(df, 'customers', my_session, use_threads=False)

table = wr.dynamodb.get_table('customers', my_session)
print(table.table_size_bytes)