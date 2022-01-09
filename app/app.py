'''
import pandas as pd
query = 'SELECT * FROM users'
conn = 'postgresql://retail_user:itversity@localhost:5452/retail_db'
df = pd.read_sql(
    query,
    conn
)

print(df)
print(df.count())
'''
import sys

'''
import pandas as pd

users_list = [
    {'user_first_name': 'Scott', 'user_last_name': 'Tiger'},
    {'user_first_name': 'Donald', 'user_last_name': 'Duck'}
]
df = pd.DataFrame(users_list)

conn = 'postgresql://retail_user:itversity@localhost:5452/retail_db'
df.to_sql('users', conn, if_exists='append', index=False)

df = pd.read_sql('SELECT * FROM users', conn)
print(df)
'''

'''
import pandas as pd

conn = 'postgresql://retail_user:itversity@localhost:5452/retail_db'
pd.read_sql('SELECT * FROM departments', conn)

BASE_DIR = '/Users/thirumeninathanmeiyappan/Desktop/thiru/Job_Prep/data_engineering_practice/hands_on/retail_db_json'
table_name = 'departments'

import os
file_name = os.listdir(f'{BASE_DIR}/{table_name}')[0]
fp = f'{BASE_DIR}/{table_name}/{file_name}'

import pandas as pd
df = pd.read_json(fp, lines=True)

conn = 'postgresql://retail_user:itversity@localhost:5452/retail_db'
df.to_sql(table_name, conn, if_exists='append', index=False)
'''

'''
import pandas as pd
query = 'SELECT * FROM departments'
conn = 'postgresql://retail_user:itversity@localhost:5452/retail_db'
df = pd.read_sql(
    query,
    conn
)

print(df)

print(df.count())

df = pd.read_sql(
	'SELECT count(1) FROM departments',
	conn
)
print(df)
'''

'''
BASE_DIR = '/Users/thirumeninathanmeiyappan/Desktop/thiru/Job_Prep/data_engineering_practice/hands_on/retail_db_json'
table_name = 'orders'

import os
file_name = os.listdir(f'{BASE_DIR}/{table_name}')[0]
fp = f'{BASE_DIR}/{table_name}/{file_name}'

import pandas as pd
json_reader = pd.read_json(fp, lines=True, chunksize=1000)

conn = 'postgresql://retail_user:itversity@localhost:5452/retail_db'

for df in json_reader:
    min_key = df['order_id'].min()
    max_key = df['order_id'].max()
    df.to_sql(table_name, conn, if_exists='append', index=False)
    print(f'Processed {table_name} with in the range of {min_key} and {max_key}')
'''

'''
import pandas as pd
query = 'SELECT * FROM orders'
conn = 'postgresql://retail_user:itversity@localhost:5452/retail_db'
df = pd.read_sql(
    query,
    conn
)

print(df)

print(df.count())

print(df.dtypes)

df = pd.read_sql(
    'SELECT order_status, count(1) AS order_count FROM orders GROUP BY order_status',
    conn
)
print(df)
'''

'''
import os
import subprocess
import read

os.environ["DB_NAME"] = "retail_db"
db_name = os.environ.get('DB_NAME')

configs = dict(os.environ.items())
print(configs['DB_NAME'])

def main():
    DB_NAME = os.environ.get('DB_NAME')
    print(f'Hello World from {DB_NAME}')

if __name__ == '__main__':
    main()
'''
import os
from read import get_json_reader
from write import load_db_table


def process_table(BASE_DIR, conn, table_name):
    json_reader = get_json_reader(BASE_DIR, table_name)
    for df in json_reader:
        load_db_table(df, conn, table_name, df.columns[0])


def main():
    BASE_DIR = os.environ.get('BASE_DIR')
    #table_name = os.environ.get('TABLE_NAME')
    table_names = sys.argv[1].split(',')
    configs = dict(os.environ.items())
    conn = f'postgresql://{configs["DB_USER"]}:{configs["DB_PASS"]}@{configs["DB_HOST"]}:{configs["DB_PORT"]}/{configs["DB_NAME"]}'
    for table_name in table_names:
        process_table(BASE_DIR, conn, table_name)


if __name__ == '__main__':
    main()