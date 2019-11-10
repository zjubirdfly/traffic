#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import os, sys
import glob
input_path = r'/home/birdfly/Taxidata/20170325'
output_path = "/home/birdfly/TaxidataResult/AllData/20170325/graph/"
os.chdir(input_path)
data_cols = ['date','time', 'label', 'tag','lat','lon','speed?','direction?','status', '?']


# In[ ]:


data_cols = ['date','time', 'label', 'tag','lat','lon','speed?','direction?','status', '?']
date_data_files = glob.glob(input_path + "/*.TXT")
date_data_files = sorted(date_data_files)   
print(len(date_data_files))

index_count = 0
sample_data = []
for date_data_file in date_data_files:
    print("**********" + date_data_file )
    sample_data.append(pd.read_csv(date_data_file, sep=',',encoding='GB2312', names= data_cols, header=None, dtype={'time': object}))

    index_count = index_count + 1
    if index_count % 2 == 0:
        sample_data = pd.concat(sample_data, axis=0, ignore_index=True)
        sample_data['tag'] = sample_data['tag'].str.replace(r'[^\x00-\x7F]+', '')
        record_data = generate_take_record_data(sample_data)
        graph = generate_map(record_data)
        
        if not os.path.exists(output_path):
            os.makedirs(output_path, mode=0o777) 
        pd.DataFrame(graph).to_csv (output_path + str(index_count / 2) + ".csv", index = None, mode = 'w',header=None)
        
        sample_data = []
    


# In[2]:


def generate_map(date_data):
    # Generate N*N graph
    N = 100

    max_lat = 106.753129
    min_lat = 106.273290
    lat_range = (max_lat - min_lat) / N

    max_lon = 29.878034
    min_lon = 29.360739
    lon_range = (max_lon - min_lon) / N

    graph = np.zeros((N,N))

    count = 0
    previous_row = None
    for index, row in date_data.iterrows(): 
      lat = row['lat']
      lon = row['lon']
      if lat > max_lat and lat < min_lat and lon > max_lon and lon < min_lon:
        continue

      #lat index
      lat_index = 0;
      if lat < min_lat:
        lat_index = 0
      elif lat > max_lat:
        lat_index = N - 1
      else:
        lat_index = int((lat - min_lat) / lat_range)
      lat_index = N - 1 - lat_index

      #lon index
      lon_index = 0;
      if lon < min_lon:
        lon_index = 0
      elif lon > max_lon:
        lon_index = N - 1
      else:
        lon_index = int((lon - min_lon) / lon_range)

      if row['status'] == 0: # take taxi
        count = count + 1
        graph[lon_index, lat_index] = graph[lon_index, lat_index] + 1
      elif row['status'] == 1: # drop off taxi
        count = count - 1
        graph[lon_index, lat_index] = graph[lon_index, lat_index] - 1

      previous_row = row
    
    return graph


# In[3]:


def generate_take_record_data(original_data):
    original_data["tag"] = original_data["tag"].str.replace(r'[^\x00-\x7F]+', '')
    original_data.sort_values(by= ['tag', 'time'] , inplace=True)
    
    # status 0 take taxi: 1 drop off taxi
    data_cols = ['tag','date','time', 'lat','lon','status']
    taxi_record_data = pd.DataFrame(columns=data_cols)
      
    previous_row = None
    for index, row in original_data.iterrows(): 
        if previous_row is not None and previous_row['tag'] == row['tag']:
            if previous_row['status'] == 0 and row['status'] == 1:
                new_record = {'tag': row['tag'], 
                               'date': row['date'], 
                               'time': row['time'], 
                               'lat': row['lat'],
                               'lon': row['lon'],
                               'status': 0}
                taxi_record_data = taxi_record_data.append(new_record, ignore_index=True)
            elif previous_row['status'] == 1 and row['status'] == 0:
                new_record = {'tag': row['tag'], 
                               'date': row['date'], 
                               'time': row['time'], 
                               'lat': row['lat'],
                               'lon': row['lon'],
                               'status': 1}
                taxi_record_data = taxi_record_data.append(new_record, ignore_index=True)         

        previous_row = row

    return taxi_record_data


# In[76]:


sample_data.sort_values(by= ['tag', 'time'] , inplace=True)


# In[77]:


sample_data


# In[ ]:




