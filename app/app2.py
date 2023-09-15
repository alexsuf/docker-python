# Стандартный импорт
import pandas as pd
import numpy as np
# Библиотека для взаимодействия с движком базы данных PostgreSQL
import pg8000
from pprint import pprint

# Извлечь из файла csv в DataFrame
df = pd.read_csv('az2.csv')
pprint(df)
# Новый DataFrame - это сумма столбцов из первого DataFrame
df_sum = df.apply(np.sum, axis=0)
print('==================')
pprint(df_sum)


# Загрузить новый DataFrame в базу данных PostgreSQL
con = pg8000.connect('host.docker.internal', 
    password='Zadonsky1')

df_sum.to_sql(name='app2', con=con)