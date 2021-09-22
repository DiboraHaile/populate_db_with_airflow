from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from load_insert import insert_data
from load_data import extract_data


default_args = {
    "owner": "admin",
    "email": "deborahaile3@gmail.com",
    "start_date": datetime(2021, 8, 20)
}

dag = DAG("el_id",default_args = default_args)



t1 = PythonOperator(task_id="extract", python_callable=extract_data,op_kwargs = {'path':"/home/dibora/airflow/data"}, dag=dag)

t2 = PythonOperator(task_id="insert_db", python_callable=insert_data, op_kwargs = {'path':"/home/dibora/airflow/sensor_data.csv"},dag=dag)

t1 >> t2

# if __name__ == "__main__":
#     df = {"name":"mom","id":859}
#     insert_data(df)

