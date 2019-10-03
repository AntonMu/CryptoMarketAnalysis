
import os
from datetime import date, datetime
import calendar
import requests
import pandas as pd 
import numpy as np
from threading import Thread 
import sqlite3
import time
import json
Data_Path = 'Data'
Result_Path = 'Results'
for path in [Data_Path,Result_Path]:
    if not os.path.exists(path):
        os.makedirs(path)

def unix_time(d):
    return calendar.timegm(d.timetuple())

def partition(pair_list,threads=4):
    np.array_split(range(len(pair_list)),threads)
    return np.array_split(range(len(pair_list)),threads)

def download_rows(pair_list,res_index=0,start=0,end=0,sleep_time=60):
    start_time = time.time()
    res_df = pd.DataFrame()
    cur_sleep_time = sleep_time
    if not end:
        end = len(pair_list)
    for index,row in pair_list[start:end].iterrows():
        crypto = row['Crypto']
        fiat = row['Fiat']
        ex = row['Exchange']
        try:
            hit_url = 'https://min-api.cryptocompare.com/data/histoday?fsym='+str(crypto)+'&tsym='+str(fiat)+'&limit=2000&aggregate=1&toTs='+str(unix_time(end_date))+'&e='+ str(ex)
            #Check for rate limit! If we hit rate limit, then wait!
            while True:
                d = json.loads(requests.get(hit_url).text)
                if d['Response'] =='Success':
                    df = pd.DataFrame(d["Data"])
                    if index%1000==0:
                        print('hitting', ex, crypto.encode("utf-8"), fiat, 'on thread', res_index) 
                    if not df.empty:
                        df['Source']=ex
                        df['From']=crypto
                        df['To']=fiat
                        df=df[df['volumeto']>0.0]
                        res_df = res_df.append(df)
                    cur_sleep_time = sleep_time
                    break
                else:
                    cur_sleep_time = int((np.random.rand()+.5)*cur_sleep_time*1.5)
                    if cur_sleep_time>1800:
                        print('Hit rate limit on thread %d, waiting for %ds'%(res_index,cur_sleep_time))
                    time.sleep(cur_sleep_time)
                
        except Exception as err:
            time.sleep(15)
            print('problem with',ex.encode("utf-8"),crypto,fiat)
    end_time = time.time()
    result_dfs[res_index] = res_df
    print('Total time spent %ds on thread %d'%(end_time-start_time,res_index))

end_date = datetime.today()

conn = sqlite3.connect(os.path.join(Data_Path,"CCC"+str(datetime.today())[:10]+".db"))

#Benchmark
pair_list = pd.read_csv(os.path.join(Data_Path,"Exchange_Pair_List.csv"))

threads = 4
parts = partition(pair_list,threads)
thread_list = [0 for _ in range(threads)]
result_dfs = [0 for _ in range(threads)]

for i, pair in enumerate(parts):
    thread_list[i] = Thread(target=download_rows, args=(pair_list,i,pair[0],pair[-1],))
for i in range(threads):
    # starting thread i 
    thread_list[i].start() 
for i in range(threads):
    thread_list[i].join() 
for result in result_dfs:
    result.to_sql("Data", conn, if_exists="append")
    print(len(result))
conn.commit()
conn.close()