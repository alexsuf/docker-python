import psycopg2
import json

def connect(params_dic):
    # NOTE подключение к серверу
    conn = None
    try:
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:            
        print(error)
        exit(1)
    return conn

# Работаем с Greenplum 

conn_gp_params = {
  'host': 'host.docker.internal', # gp
  'database': 'az',
  'user': 'gpadmin',
  'password': 'gpadmin',
  'port': '54322' # 5432
}
conn_gp = connect(conn_gp_params)
cur2 = conn_gp.cursor()
cur2.execute("select * from dev.product;")
myrec2 = cur2.fetchall()

for a in myrec2:
  print(a)

conn_gp.commit()
conn_gp.close()
print ('Ok - Greenplum')


conn_postgres_params = {
  'host': 'host.docker.internal', 
  'database': 'shop',
  'user': 'postgres',
  'password': 'Zadonsky1',
  'port': '54321'
}
conn_postgres = connect(conn_gp_params)
  
cur = conn_postgres.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS test_1 (id serial PRIMARY KEY, num integer, data varchar, form_data jsonb);")
conn_postgres.commit()

def gen_records():
    for row_idx in range(10_000):
        yield (row_idx, 'txt ' + str(row_idx), json.dumps({"foo": "bar " + str(row_idx)}))

for record in gen_records():
    cur.execute("INSERT INTO test_1 (num, data, form_data) VALUES (%s, %s, %s)", record)

conn_postgres.commit()

print ('Table inserted')

cur.execute("SELECT count(1) FROM test_1;")
print(cur.fetchone())

cur.execute("SELECT * FROM test_1 where 1=1 limit 20;")
myrec = cur.fetchall()

for a in myrec:
  print(a)
cur.close()


print ('Thats all for today')