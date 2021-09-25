
import pandas as pd
import mysql.connector
from mysql.connector import Error
import os


class Load:
    def __init__(self,path,database_name,table_type) -> None:
        self.path = path
        self.db = database_name
        self.table_type = table_type

    def load_to_table(self):
        self.load_extracted_data()
        self.insert_table()
    
    def load_extracted_data(self):
        self.df = pd.read_csv(self.path)
        self.lanes = int((len(self.df.columns)-1)/3)

    def create_connection(self):
        connection = mysql.connector.connect(host='localhost',
                                                database=self.db,
                                                user='root',
                                                password='adminadmin')

        cursor = connection.cursor()
        return connection, cursor

    def create_insert_query(self):
        mysql_insert_query = ""
        if self.table_type == "lane":
            table_name = "Lane_"+str(self.lanes)+"_table"
            no_values_row = (self.lanes*3)+1
            col_names_list = [j+"_"+str(i) for j in ['flow','occupation','speed'] for i in range(1,self.lanes+1)]
            col_names = ",".join(col_names_list)
            mysql_insert_query = """INSERT INTO """+table_name+""" (L"""+str(self.lanes)+"_station_ID,"+col_names+""") VALUES ("""+",".join(["%s" for i in range(no_values_row)])+");" 

        elif self.table_type == "station":
            table_name = "station_info_table"
            mysql_insert_query = """INSERT INTO """+table_name+"""(District_ID,ID,Lanes,Name) VALUES (%s,%s,%s,%s);"""
            no_values_row = 4

        elif self.table_type == "traffic":
            table_name = "traffic_info_table"
            mysql_insert_query = """INSERT INTO """+table_name+"""(date,time,traffic_station_ID) VALUES (%s,%s,%s);"""
            no_values_row = 3

        return mysql_insert_query,no_values_row

    def insert_table(self):
            connection, cursor = self.create_connection()
            query,no_values_row = self.create_insert_query()
            
            list_rows = []
            for _,row in self.df.iterrows():
                list_rows.append(tuple([row[i] for i in range(no_values_row)]))
            cursor.executemany(query, list_rows)
            connection.commit()
            print(cursor.rowcount, "Record inserted successfully into the table")
            cursor.close()


        
             
    