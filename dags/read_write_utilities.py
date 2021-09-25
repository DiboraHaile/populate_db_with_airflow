import pandas as pd
import os
import numpy as np

def txt_to_csv(path):
    """
    convert text data to csv file
    Args:
    -----
    path: str - path of directory containing text files

    Returns
    -------
    pandas data frame
    """

    df_list = []
    
    for filename in os.listdir(path):
        if filename.endswith(".txt"):
            print(filename)
            df = pd.read_csv(os.path.join(path,filename))
            df_list.append(df)

    if len(df_list) == 0:
        print("no text file found")
    df_concat = pd.concat(df_list,ignore_index=True)
    df_concat.to_csv('sensor_data.csv',index = None)
        
    return df_concat

def load_txt(path_raw):
    with open(path_raw) as f:
        text_list = f.readlines()
    return text_list

def to_file_csv(df,destination_path):
    df.to_csv(destination_path,index = None)
