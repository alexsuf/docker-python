import psycopg2
import string
from pprint import pprint
from random import choice

def connect(params_dic):
    # NOTE подключение к серверу
    conn = None
    try:
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:            
        print(error)
        exit(1)
    return conn

def generate_login(uid):
    name = choice (string.ascii_lowercase)
    surname = "".join(choice(string.ascii_lowercase) for _ in range(5))
    return f"{name}_{uid}_{surname}"

def generate_name():
    return "".join(choice(string.ascii_lowercase) for _ in range(6)).title()


# Подключаемся к базе 
conn_postgres_params = {
    'host': 'host.docker.internal', 
    'database': 'shop',
    'user': 'postgres',
    'password': 'Zadonsky1',
    'port': '54321'
    }
conn_postgres = connect(conn_postgres_params)
  
cur = conn_postgres.cursor()

# Создаем таблицу в app базе shop
cur.execute("CREATE TABLE IF NOT EXISTS app ( \
            uid   int not null, \
            login text not null, \
            name  text not null \
            );")

# Генерируем случайные данные для таблицы
# и заносим их в базу данных
for i in range (1, 101):
    cur.execute("INSERT INTO app (uid, login, name) \
               VALUES (%s, %s, %s)", \
               (i, generate_login(i), generate_name()))

cur.execute("SELECT * FROM app LIMIT 10;")

# Считываем данные
sql_data = cur.fetchall()
print("-------------------------")
pprint(sql_data)
print("-------------------------")
# Выводим их на экран
for a in sql_data:
  print(a)
cur.close()
print("-------------------------")
conn_postgres.commit()
cur.close()
conn_postgres.close()

print ('Thats all for today')