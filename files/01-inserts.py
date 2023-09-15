import psycopg2
from pprint import pprint

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
    port='54321',
    database="shop",
    user="postgres",
    password="Zadonsky1") 
  
  
cur = con.cursor()
print ('Ok')

cur.execute("SELECT task FROM dev.app_tasks")
sql_data = cur.fetchall()
print("-------------------------")
pprint(sql_data)
print("-------------------------")

print ('Finished')

con.commit()
con.close()
