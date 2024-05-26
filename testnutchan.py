from pymelsec import Type3E
from pymelsec.constants import DT
import time
from datetime import datetime
import pandas as pd
import os
import threading
import json
import paho.mqtt.client as mqtt

is_raspberry = 0

if is_raspberry:
    header_topic_mqtt = '/home/'
else:
    header_topic_mqtt = 'home/'
lock = threading.Lock()

def create_excel_file(file_name):
    try:
        with open(header_topic_mqtt + 'pi/' + file_name) as store_data:
            pass
    except FileNotFoundError:
        with open(header_topic_mqtt + 'pi/' + file_name,'w+') as store_data:
            store_data.write('{0},{1},{2}\n'.format('Name Variable','Value','Timestamp'))
create_excel_file('stored_data.csv')

#---------------------------------------------------------------------------
__HOST = '192.168.1.250' # REQUIRED
__PORT = 4095           # OPTIONAL: default is 5007
__PLC_TYPE = 'Q'     # OPTIONAL: default is 'Q'

while True:
    if is_raspberry:
        res = os.system('ping -c 1 ' + str(__HOST) + ' > /dev/null 2>&1')
    else:
        res = os.system('ping -n 1 ' + str(__HOST) + ' > nul')
    time.sleep(1)
    if res == 0:
        print('Connected to the ip')
        break
    else:
        print("Can't connect to PLC")

try:
    plc = Type3E(host=__HOST, port=__PORT, plc_type=__PLC_TYPE)
    plc.set_access_opt(comm_type='binary')
    plc.connect(ip=__HOST, port=__PORT)
except Exception as e:
    print(e)
#---------------------------------------------------------------------------
while True:
    with lock:
        read_result = plc.batch_read(
            ref_device='X22',
            read_size=1,
            data_type=DT.BIT
        )
        RealValue = read_result[0].value
        print(RealValue)
        Timestamp = datetime.now().isoformat(timespec='microseconds')
        with open(header_topic_mqtt + 'pi/stored_data.csv','a+') as store_data:
            store_data.write('{0},{1},{2}\n'.format('X22', RealValue, Timestamp))

