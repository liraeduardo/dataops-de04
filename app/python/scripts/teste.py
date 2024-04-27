import os
from sqlalchemy import create_engine, URL, text
import mysql.connector
import time
import sys
# print(os.environ['DB_PASSWORD'])

url = URL.create(
            'mysql+mysqlconnector',
            username='root',
            password='root',
            host='localhost',
            port='3306',
            database='db'
        )

# print(url)
engine = create_engine(url)
start = time.time()
timeout = 2

while time.time() - start <= timeout:
    try:
        time.sleep(1)
        engine.connect()
        print('conexão ok')
        engine.connect().close()
        sys.exit()
    except Exception as e:
        print(e)
        print('nova tentativa em segundos...')

# result = engine.connect().execute(text('select 1'))
# for i in result:
#     print(i)

# print(**configs)
# con = mysql.connector.connect(user='root', **configs)

# con.close()

