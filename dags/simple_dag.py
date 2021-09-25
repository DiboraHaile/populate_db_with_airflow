from datetime import datetime, timedelta
from os import path
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from extract_data import Extract
from load_insert_data import Load

def insert_data(filename,table_type,db_name):
    ld = Load(filename,db_name,table_type)
    ld.load_to_table()     

def load_extract_data(path_raw_data,path_metadata):
    ex = Extract(path_raw_data,path_metadata)
    ex.extract_data()


default_args = {
    "owner": "admin",
    "email": "deborahaile3@gmail.com",
    "start_date": datetime(2021, 8, 20)
}

dag = DAG("el_tag_new",default_args = default_args)



t1 = PythonOperator(task_id="extract", python_callable=load_extract_data,op_kwargs = {"path_raw_data":"/home/dibora/airflow/data/davis10.txt","path_metadata":"/home/dibora/airflow/data/I80_stations.csv"}, dag=dag)

t2 = PythonOperator(task_id="insert_station_data", python_callable=insert_data, op_kwargs = {'filename':"/home/dibora/airflow/data/extracted/station_metadata.csv","table_type":"station","db_name":"Sensor_raw_db"},dag=dag)
t3 = PythonOperator(task_id="insert_traffic_data", python_callable=insert_data, op_kwargs = {'filename':"/home/dibora/airflow/data/extracted/traffic_data.csv","table_type":"traffic","db_name":"Sensor_raw_db"},dag=dag)
t4 = PythonOperator(task_id="insert_raw_data", python_callable=insert_data, op_kwargs = {'filename':"/home/dibora/airflow/data/extracted/lane_data/data_lane4.csv","table_type":"lane","db_name":"Sensor_raw_db"},dag=dag)

t1 >> t2 >> t3 >> t4
