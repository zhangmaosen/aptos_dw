import json
import csv 
from datetime import datetime
import resource
file_name = "2_test.json";
etl_file_name = "aptos_etl_trans.csv";

f = open(file_name, "rb");
etl_f = open(etl_file_name, "w");
csv_writer = csv.writer(etl_f);

etl_header = [
    'timestamp',
    'block_height',
'transaction_version', 
'sender_addr',
'type',
'payload_func',
'resource_addr', 
'resource_func_module', 
'resource_func',
'receiver_addr',
'coin_type',
'coin_amount',
'gas_used'];

def etl_token_data(data_json):
    

csv_writer.writerow(etl_header);
for line in f:
    # print(line);
    try :
        block = json.loads(line);
    except:
        print(line);
        #break;
    #print(block["block_height"])
    line_data = [];
    block_height = block["block_height"];
    txs = block['transactions'];
    ts = int(block['block_timestamp'])/1000000;
    #print(ts);
    ts = datetime.fromtimestamp(ts) ;
    date_str = ts.strftime("%d-%m-%Y, %H:%M:%S");
    for i in txs:
        line_data = [];
        coin_type = "NULL";
        resource_addr = "NULL";
        resource_func_module = "NULL";
        resource_func = "NULL";
        #print(i['version']);
        version = i['version'];
        sender_addr = i['sender'] if 'sender' in i else 'NULL';
        txs_type = i['type'];
        ## account_trans
        pay_load_func = i['payload']['function'] if 'payload' in i and 'function' in i['payload']  else 'NULL';
        if (pay_load_func != "NULL"):
            [resource_addr, resource_func_module, resource_func] = pay_load_func.split("::",3);
        if(pay_load_func == '0x1::aptos_account::transfer' ):
            coin_type = '0x1::aptos_coin::AptosCoin';
            receiver = i['payload']['arguments'][0];
            coin_amount = i['payload']['arguments'][1];
            
            ##print(coin_amount);
        else:
            ## coin_trans
            if(pay_load_func == '0x1::coin::transfer' ):
                coin_type = i['payload']['type_arguments'][0];
                receiver = i['payload']['arguments'][0];
                coin_amount = i['payload']['arguments'][1];
            else:
                receiver = 'NULL';
                coin_amount = 'NULL';

        gas_used = i['gas_used'];
        line_data = line_data + [
            date_str,
            block_height, 
            version, 
            sender_addr, 
            txs_type, 
            pay_load_func,
            resource_addr, 
            resource_func_module, 
            resource_func,
            receiver,
            coin_type,
            coin_amount,
            gas_used];

        #print(line_data);
        csv_writer.writerow(line_data);

etl_f.close();