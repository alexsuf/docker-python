import psycopg2
import psycopg2.extras as extras
import numpy as np
import pandas as pd
import datetime

  
def connect(params_dic):
    # NOTE подключение к серверу
    conn = None
    try:
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:            
        print(error)
        exit(1)
    return conn
    
    def etl_categories_changes(**kwargs):

        conn_gp_params = {
            'host': '10.4.49.6',
            'database': 'datatech_zadonsky',
            'user': 'airflow',
            'password': 'airflow',
        }

        conn_gp = connect(conn_gp_params)
        conn_gp.cursor().execute('select * from dev.load_categories();')
        conn_gp.commit()
        conn_gp.close()

    task_1_etl_categories_changes = PythonOperator(
        task_id='task_1_etl_categories_changes',
        python_callable=etl_categories_changes,
        op_kwargs={},
    )

    def etl_clients_changes(**kwargs):

        conn_gp_params = {
            'host': '10.4.49.6',
            'database': 'datatech_zadonsky',
            'user': 'airflow',
            'password': 'airflow',
        }
        
        conn_gp = connect(conn_gp_params)
        conn_gp.cursor().execute('select * from dev.load_clients();')
        conn_gp.commit()
        conn_gp.close()

    task_2_etl_clients_changes = PythonOperator(
        task_id='task_2_etl_clients_changes',
        python_callable=etl_clients_changes,
        op_kwargs={},
    )
    
    def etl_subscriptions_changes(**kwargs):

        conn_gp_params = {
            'host': '10.4.49.6',
            'database': 'datatech_zadonsky',
            'user': 'airflow',
            'password': 'airflow',
        }
        
        conn_gp = connect(conn_gp_params)
        conn_gp.cursor().execute('select * from dev.load_transaction_sub();')
        conn_gp.commit()
        conn_gp.close()

    task_3_etl_subscriptions_changes = PythonOperator(
        task_id='task_3_etl_subscriptions_changes',
        python_callable=etl_subscriptions_changes,
        op_kwargs={},
    )
    
    def etl_transactions_to_stage(**kwargs):

        conn_gp_params = {
            'host': '10.4.49.6',
            'database': 'datatech_zadonsky',
            'user': 'airflow',
            'password': 'airflow',
        }
        
        conn_gp = connect(conn_gp_params)
        conn_gp.cursor().execute('select * from dev.load_transactions_to_stage();')
        conn_gp.commit()
        conn_gp.close()

    task_4_etl_transactions_changes = PythonOperator(
        task_id='task_4_etl_transactions_to_stage',
        python_callable=etl_transactions_to_stage,
        op_kwargs={},
    )
    
    def etl_transactions_h(**kwargs):

        conn_gp_params = {
            'host': '10.4.49.6',
            'database': 'datatech_zadonsky',
            'user': 'airflow',
            'password': 'airflow',
        }
        
        conn_gp = connect(conn_gp_params)
        conn_gp.cursor().execute('select * from dev.load_transactions_h();')
        conn_gp.commit()
        conn_gp.close()

    task_5_etl_transactions_clients_changes = PythonOperator(
        task_id='task_5_etl_transactions_h',
        python_callable=etl_transactions_h,
        op_kwargs={},
    )
    
    def etl_transactions_hs(**kwargs):

        conn_gp_params = {
            'host': '10.4.49.6',
            'database': 'datatech_zadonsky',
            'user': 'airflow',
            'password': 'airflow',
        }
        
        conn_gp = connect(conn_gp_params)
        conn_gp.cursor().execute('select * from dev.load_transactions_hs();')
        conn_gp.commit()
        conn_gp.close()

    task_6_etl_transactions_categories_changes = PythonOperator(
        task_id='task_6_etl_transactions_hs',
        python_callable=etl_transactions_hs,
        op_kwargs={},
    )
    
    def etl_transactions_l(**kwargs):

        conn_gp_params = {
            'host': '10.4.49.6',
            'database': 'datatech_zadonsky',
            'user': 'airflow',
            'password': 'airflow',
        }
        
        conn_gp = connect(conn_gp_params)
        conn_gp.cursor().execute('select * from dev.load_transactions_l();')
        conn_gp.commit()
        conn_gp.close()

    task_7_etl_transactions_categories_changes = PythonOperator(
        task_id='task_7_etl_transactions_l',
        python_callable=etl_transactions_l,
        op_kwargs={},
    )    

   def etl_transactions_r(**kwargs):

        conn_gp_params = {
            'host': '10.4.49.6',
            'database': 'datatech_zadonsky',
            'user': 'airflow',
            'password': 'airflow',
        }
        
        conn_gp = connect(conn_gp_params)
        conn_gp.cursor().execute('select * from dev.load_transactions_r();')
        conn_gp.commit()
        conn_gp.close()

    task_8_etl_transactions_categories_changes = PythonOperator(
        task_id='task_8_etl_transactions_r',
        python_callable=etl_transactions_r,
        op_kwargs={},
    ) 

    task_0_dummy >> task_1_etl_categories_changes
    task_0_dummy >> task_2_etl_clients_changes
    task_0_dummy >> task_3_etl_subscriptions_changes
    task_0_dummy >> task_4_etl_transactions_to_stage >> task_5_etl_transactions_h
    task_0_dummy >> task_4_etl_transactions_to_stage >> task_6_etl_transactions_hs
    task_0_dummy >> task_4_etl_transactions_to_stage >> task_7_etl_transactions_l
    task_0_dummy >> task_4_etl_transactions_to_stage >> task_8_etl_transactions_r

    