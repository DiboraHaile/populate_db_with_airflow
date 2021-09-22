import pandas as pd
import os

def extract_data(path):
    """
    extract text data from the filename proposed and return
    pandas dataframe
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

