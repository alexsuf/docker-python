from flask import Flask, render_template, request
import psycopg2
from pprint import pprint


app = Flask(__name__, template_folder='templates')

# Подключение к базе данных
conn = psycopg2.connect(
    host="host.docker.internal",
    port='1111',
    database="shop",
    user="postgres",
    password="Zadonsky1"
)

# Определение курсора
cur = conn.cursor()

# Определение маршрута для вывода списка задач
@app.route('/')
def index():
    # Выборка всех задач из таблицы dev.app_tasks
#    cur.execute("SELECT id, task FROM dev.app_tasks")
    cur.execute("select id, CONCAT(' (', TO_CHAR(dtime, 'DD.MM.YYYY'), ') --> ', task) from dev.app_tasks")
    rows = cur.fetchall()
    pprint(rows)
    # Вывод списка задач на экран
    return render_template('index.html', rows=rows)

# Определение маршрута для удаления задачи
@app.route('/delete', methods=['POST'])
def delete():
    # Получение id задачи из запроса
    task_id = request.form['task_id']
    print ('task_id = ', task_id)
    # Удаление задачи из таблицы dev.app_tasks
    cur.execute("DELETE FROM dev.app_tasks WHERE id = %s", (task_id,))
    conn.commit()
    # Перенаправление на главную страницу
    return index()

# Определение маршрута для добавления задачи
@app.route('/add', methods=['POST'])
def add():
    # Получение текста задачи из запроса
    task_text = request.form['task_text']
    # Добавление задачи в таблицу dev.app_tasks
    cur.execute("INSERT INTO dev.app_tasks (task) VALUES (%s)", (task_text,))
    conn.commit()
    # Перенаправление на главную страницу
    return index()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)