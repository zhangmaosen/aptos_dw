from time import strftime
from unittest.util import strclass
import requests
import json
import threading
import time

api_url_base = "https://fullnode.mainnet.aptoslabs.com/v1/blocks/by_height/"
with_txs = "with_transactions=true";
st_block_height=1600000;
target_count = 5000;
thread_count = 3;
ts_name = time.time();
# response = requests.get(api_url);
# print(response.json())

def get_data(thread_num, start_height, count):
    print("Thread working: " + str(thread_num));
    file_name_base="aptos_mainnet_";
    file_name = file_name_base+str(int(ts_name))+"_"+str(start_height)+"_"+str(count)+".json";
    f= open(file_name,"w");
    block_height = start_height;
    user_agent = {'User-agent': 'Mozilla/5.0'};
    while block_height < start_height + count:
        api_url = api_url_base + str(block_height)+"?"+ with_txs;
        # print(api_url);
        # response = requests.get(api_url);
        # print(response.json())
        #print(file_name_base+str(block_height)+".json");

        print("thread: "+str(thread_num)+" data get from "+api_url);

        response = requests.get(api_url, headers= user_agent);
        if response.status_code == 200:
            json.dump(response.json(), f);
            f.write("\n");
            block_height += 1;
        else:
            print(response);
            time.sleep(90);
    f.close();

try:
    threads = [];
    for i in range(thread_count):
        thread = threading.Thread(target=get_data, args=(i, st_block_height+i*target_count, target_count));
        threads.append(thread);
        thread.start();
   #thread2 = threading.Thread(target=get_data, args=(2, block_height+target_count, target_count))
except:
    print("Error! thread!")
# get_data(1, block_height, target_count);


for i in threads:
    i.join();
