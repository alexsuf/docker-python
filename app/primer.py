import psycopg2
from pprint import pprint
import pandas as pd
import numpy as np

def connect(params_dic):
    # NOTE подключение к серверу
    conn = None
    try:
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:            
        print(error)
        exit(1)
    return conn

con = psycopg2.connect(
    host="host.docker.internal",
    port='1111',
    database="shop",
    user="postgres",
    password="Zadonsky1") 

cur = con.cursor()
print ('Ok')

cur.execute("select id, task, TO_CHAR(dtime, 'DD.MM.YYYY') from dev.app_tasks")
sql_data = cur.fetchall()
print("-------------------------")
pprint(sql_data)
print("-------------------------")



print ('Finished')

con.commit()
con.close()
