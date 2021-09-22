# from load_insert_db_dag import create_dag_script 
# from simple_dag import print_hello
import pandas as pd
import mysql.connector
from mysql.connector import Error
# db_dag = create_dag_script('admin','load_data','el_dag','2014-1-13')
# t1 = db_dag.create_task(print_hello,{'msg':"hello"})



def insert_data(path):
    df = pd.read_csv(path)
    connection = mysql.connector.connect(host='localhost',
                                            database='sensor_db',
                                            user='root',
                                            password='adminadmin')

    try:
        if connection.is_connected():
            print("Connected to MySQL Server")
            cursor = connection.cursor()

            mySql_insert_query = """INSERT INTO I80_data_table (date_recorded,time,station_id,col3,col4 ,col5,col6,col7,col8,
                                                                col9,col10,col11,col12,col13,col14,col15,col16,col17,
                                                                col18,col19,col20,col21,col22,col23,col24,col25,col26) 
                           VALUES (%s, %s,%s, %s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s) """
            
            list_rows = []
            for _,row in df.iterrows():
                list_rows.append((row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],
                                 row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],
                                 row[16],row[17],row[18],row[19],row[20],row[21],row[22],row[23],
                                 row[24],row[25],row[26]))

            cursor.executemany(mySql_insert_query, list_rows)
            connection.commit()
            print(cursor.rowcount, "Record inserted successfully into employee_data table")
            cursor.close()
            

        
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    insert_data("/home/dibora/airflow/sensor_data.csv")
#     df = {"name":"tensi","id":89}
#     insert_data(df)
