
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
import random
from lxml.html import fromstring
import requests
from itertools import cycle
import traceback


Data_Path = 'Data'
Result_Path = 'Results'
for path in [Data_Path,Result_Path]:
    if not os.path.exists(path):
        os.makedirs(path)

def unix_time(d):
    return calendar.timegm(d.timetuple())

def get_proxies(number = 1000):
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    for i in parser.xpath('//tbody/tr')[:number]:
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
            proxies.add(proxy)
    return proxies

def partition(pair_list,threads=4,shuffle=False):
    pair_len = len(pair_list)
    if shuffle:
        nums = list(range(len(pair_list)))
        random.shuffle(nums)
        return  np.array_split(nums,threads)
    else:
        #We split it evenly
        splits = []
        for i in range(threads):
            splits.append(list(range(i,pair_len,threads)))
        return splits

def download_rows(pair_list,thread_index=0,index_range=np.array([]),sleep_time=15,threads=100):
    proxies = get_proxies()
    proxy_pool = cycle(proxies)
    proxy = next(proxy_pool)
    
    start_time = time.time()
    res_df = pd.DataFrame()
    cur_sleep_time = sleep_time
    if not np.array(index_range).any():
        #if not given, do everything
        index_range = range(len(pair_list))
        print('Using the full range')
    for index,row in pair_list.iloc[index_range].iterrows():
        crypto = row['Crypto']
        fiat = row['Fiat']
        ex = row['Exchange']
#         try:
        hit_url = 'https://min-api.cryptocompare.com/data/histoday?fsym='+str(crypto)+'&tsym='+str(fiat)+'&limit=2000&aggregate=1&toTs='+str(unix_time(end_date))+'&e='+ str(ex)
        #Check for rate limit! If we hit rate limit, then wait!
        counter = 0
        while True:
            try:
                d = json.loads(requests.get(hit_url,proxies={"http": proxy, "https": proxy}).text)
                if d['Response'] =='Success':
                    df = pd.DataFrame(d["Data"])
                    if (index-thread_index)%(1000*threads)==0: #We need to offset the thread index or else only the first index range will get hit
                        print('hitting', ex, crypto.encode("utf-8"), fiat, 'on thread', thread_index) 
                    if not df.empty:
                        df['Source']=ex
                        df['From']=crypto
                        df['To']=fiat
                        df=df[df['volumeto']>0.0]
                        res_df = res_df.append(df)
                    cur_sleep_time = sleep_time
                    counter = 0
                    break
                else:
                    time.sleep(int((np.random.rand()+.5)*sleep_time))
                    proxy = next(proxy_pool)
                    counter +=1
                    if counter%100==0:
                        #Refresh proxy list
                        proxies = get_proxies()
                        proxy_pool = cycle(proxies)
                        print('Hit rate limit while connecting to proxy %s on thread %d for %d times'%(str(proxy),thread_index,counter))
                        if counter>1000:
                            #If nothing is happening after such a long time, we just skip it
                            print('Problem with', ex, crypto.encode("utf-8"), fiat, 'on thread', thread_index)
                            counter = 0
                            break 
            except Exception as err:
                proxy = next(proxy_pool)
                counter +=1
                if counter%100==0:
                    #Refresh proxy list
                    proxies = get_proxies()
                    proxy_pool = cycle(proxies)
                    print('Unable to connect to proxy %s on thread %d for %d times'%(str(proxy),thread_index,counter))
    end_time = time.time()
    result_dfs[thread_index] = res_df
    print('Total time spent %ds on thread %d'%(end_time-start_time,thread_index))

end_date = datetime.today()

conn = sqlite3.connect(os.path.join(Data_Path,"CCC"+str(datetime.today())[:10]+".db"))

#Benchmark
pair_list = pd.read_csv(os.path.join(Data_Path,"Exchange_Pair_List.csv"))

threads = 100
parts = partition(pair_list,threads,shuffle=False)
thread_list = [0 for _ in range(threads)]
result_dfs = [0 for _ in range(threads)]

for i, part in enumerate(parts):
    thread_list[i] = Thread(target=download_rows, args=(pair_list,i,part,))
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