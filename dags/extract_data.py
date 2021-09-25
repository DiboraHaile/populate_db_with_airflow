
from datetime import date
import pandas as pd
import numpy as np
from read_write_utilities import load_txt,to_file_csv

# I have 7 tables right so the logic behind it is we take the raw_data and concatenate
# with station data
# then loop for data points and insert the date,station_id to traffic_data_table
# then look at the number of lanes and insert the others to the right table
# and name,id,districts,lane to station_info_table


class Extract:
    def __init__(self,path_raw_data,path_station_metadata):
        self.path_raw = path_raw_data
        self.path_station = path_station_metadata
        self.dict_df = {}
        self.rows = []
        self.lanes = []
        
    
    def extract_data(self):
        self.extract_raw_data()
        self.extract_station_meta_data()
        

    def extract_station_meta_data(self):
        df_metadata = self.filter_meta_data()
        to_file_csv(df_metadata,"/home/dibora/airflow/data/extracted/station_metadata.csv")

    def extract_raw_data(self):
        self.return_lanes()
        self.filter_data_by_lane()
        dfs = self.create_dfs()
        self.extract_traffic_data()
        for df,lane in zip(dfs,self.lanes):
            to_file_csv(df,"/home/dibora/airflow/data/extracted/lane_data/data_lane"+str(int(lane))+".csv")
    
    def extract_traffic_data(self):
        traffic_data = np.array(self.list_traffic_data)
        df_traffic_data = pd.DataFrame({'date':traffic_data[:,0],'time':traffic_data[:,1],'District_ID':traffic_data[:,2]})
        df_traffic_data['date'] = pd.to_datetime(df_traffic_data['date'])
        df_traffic_data['time'] = pd.to_datetime(df_traffic_data['time'])
        to_file_csv(df_traffic_data,"/home/dibora/airflow/data/extracted/traffic_data.csv")
    
    def create_dfs(self):
        dfs = []
        for key in self.dict_df.keys():
            df = pd.DataFrame(self.dict_df[key])
            # df.dropna(inplace=True)
            dfs.append(df)
        return dfs 


    def return_lanes(self):
        self.list_traffic_data = []
        text_list = load_txt(self.path_raw)
        for val in text_list:
            row_as_list = val.split(",")
            corrected_row = row_as_list[0].split()+row_as_list[1:]
            no_null = [value for value in corrected_row if value != "" and value != "\n"]
            lane = (len(no_null)-3) / 3
            self.rows.append([no_null[2:],lane])
            self.lanes.append(lane)
            self.list_traffic_data.append(no_null[:3])
        self.lanes = np.unique(self.lanes)

    def filter_data_by_lane(self):
        for lane in self.lanes:
            container = []
            for row,row_lane in self.rows:
                if row_lane == lane:
                    container.append(row)
            self.dict_df[lane] = container

    def filter_meta_data(self):
        df = pd.read_csv(self.path_station)
        return df[['District','ID','Lanes','Name']]

    def return_dict_df(self):
        return self.dict_df

