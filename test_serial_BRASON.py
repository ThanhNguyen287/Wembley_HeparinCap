from pymelsec import Type3E
from pymelsec.constants import DT
import paho.mqtt.client as mqtt
import time
import pandas as pd
import os, sys
import threading
from datetime import datetime, timedelta
import json
from mqtt_spb_wrapper import*
import asyncio
import serial

# time.sleep(20)
lock = threading.Lock()
is_raspberry = 0

if is_raspberry:
    header_topic_mqtt = '/home/'
else:
    header_topic_mqtt = 'home/'

'''
    Quy định trạng thái:
    0: On (Khi vừa bật máy)
    1: Run (Running)
    2: Idle (trạng thái ngừng do không có đơn hàng, lúc đầu máy Run nhưng sau đó ngưng nhưng không tắt máy)
    3. Alarm
    4. Setup (bảo trì)
    5. Off
    6. Ready (khi power on và ko bị idle, ko bị fault)
    7. Wifi disconnect

'''
#Serial_BRASON
ser = serial.Serial(
    # port='/dev/serial0',    # module RS485 thường
    # port='/dev/ttyUSB0',    # module RS485 USB
    port='COM8',    # module RS485 USB
    baudrate=9600,
    timeout=0.050
)


is_connectWifi = 0
status_old = -1


def create_excel_file(file_name):
    try:
        with open(header_topic_mqtt + 'pi/' + file_name) as store_data:
            pass
    except FileNotFoundError:
        with open(header_topic_mqtt + 'pi/' + file_name,'w+') as store_data:
            store_data.write('{0},{1},{2},{3},{4},{5},{6}\n'.format('No.','ID','Name Variable','Value','Timestamp','Kind of Data','Connected Wifi'))
create_excel_file('stored_serial.csv')


#---------- Generate Json Payload -------------------------------
def generate_data_status(state, value):
	data = [{
                'name': 'machineStatus',
                'value': value,
                'timestamp': datetime.now().isoformat(timespec='microseconds')
	}]
	return (json.dumps(data))


def generate_data(data_name, data_value):
	data = [{
                'name': str(data_name),
                'value': data_value,
                'timestamp': datetime.now().isoformat(timespec='microseconds')
	}]
	return (json.dumps(data))


def generate_data_disconnectWifi(data_name, data_value, timestamp):
	data = [{
                'name': str(data_name),
                'value': data_value,
                'timestamp': timestamp
	}]
	return (json.dumps(data))

#-------------------------------------------------------------

#-------------------------------------------------------------

# topic_standard = 'HCM/IE-F2-HCA01/Metric/'
# topic_standard = 'Test/IE-F2-HCA01/Data/'
topic_desktop_app = 'WembleyMedical/HCM/IE-F2-HCA01/Desktop/'

#----------------Store and Publish Functions------------------
def store_and_pubish_data(No, Name, Device, RealValue, KindOfData):
    global is_connectWifi
    with lock:
        Timestamp = datetime.now().isoformat(timespec='microseconds')
        with open(header_topic_mqtt + 'pi/stored_serial.csv','a+') as store_data:
            store_data.write('{0},{1},{2},{3},{4},{5},{6}\n'.format(No, Device, Name, RealValue, Timestamp, KindOfData, is_connectWifi))

        data = generate_data(Name, RealValue)
        mqtt_topic = topic_desktop_app + str(Name)
        print(data)
        client.publish(mqtt_topic,str(data),1,1)


#-------------------------------------------------------------

# --------------------------- Setup MQTT -------------------------------------
# Define MQTT call-back function
def on_connect(client, userdata, flags, rc):
    print('Connected to MQTT broker with result code ' + str(rc))


def on_disconnect(client, userdata, rc):
    global is_connectWifi
    if rc != 0:
        print('Unexpected disconnection from MQTT broker')
        is_connectWifi = 0


mqttBroker = '40.82.154.13'  # cloud
# mqttBroker = '10.0.70.45'  # cloud
mqttPort = 1883
mqttKeepAliveINTERVAL = 45

# Initiate Mqtt Client
client = mqtt.Client()
# Connect with MQTT Broker
print('connecting to broker ',mqttBroker)
# Check connection to MQTT Broker 
try:
	client.connect(mqttBroker, mqttPort, mqttKeepAliveINTERVAL)
except:
	print("Can't connect MQTT Broker!")
	
client.loop_start()
time.sleep(1)


def analyze_string(data, count):
    # Split the string using ',' delimiter
    parts = data.split(',')
    parts = [part.replace('.',',') for part in parts]
    print(parts)
    if len(parts) != 15:
        return

    Cycle = parts[0]
    Timestamp = parts[1]
    Date = parts[2]
    RunTime = parts[3]
    Pk_Pwr = parts[4]
    Energy = parts[5]
    Weld_Abs = parts[6]
    Weld_Col = parts[7]
    Total_Col = parts[8]
    TrigForce = parts[9]
    Weld_Force = parts[10]
    Freq_Chg = parts[11]
    Set_AMP_A = parts[12]
    Velocity = parts[14]


        # # Print the extracted components
    data_dict_Tr1vs3_S7 = {
        'Weld_Cycle_Tr1&3_S7': Cycle,
        'Timestamp': Timestamp,
        'Date': Date,
        'RunTime_Tr1&3_S7': RunTime,
        'Pk_Pwr_Tr1&3_S7': Pk_Pwr,
        'Energy_Tr1&3_S7': Energy,
        'Weld_Abs_Tr1&3_S7': Weld_Abs,
        'Weld_Col_Tr1&3_S7': Weld_Col,
        'Total_Col_Tr1&3_S7': Total_Col,
        'Trig_Force_Tr1&3_S7': TrigForce,
        'Weld_Force_Tr1&3_S7': Weld_Force,
        'Freq_Chg_Tr1&3_S7': Freq_Chg,
        'Set_AMP_A_Tr1&3_S7': Set_AMP_A,
        'Velocity_Tr1&3_S7': Velocity
    }

    for name, value in data_dict_Tr1vs3_S7.items():
        store_and_pubish_data(count, name, name, value, 'UltraSonic Machine')

    # data_dict_Tr2vs4_S6 = {
    #     'Weld_Cycle_Tr2&4_S6': Cycle,
    #     'Timestamp': Timestamp,
    #     'Date': Date,
    #     'RunTime_Tr2&4_S6': RunTime,
    #     'Pk_Pwr_Tr2&4_S6': Pk_Pwr,
    #     'Energy_Tr2&4_S6': Energy,
    #     'Weld_Abs_Tr2&4_S6': Weld_Abs,
    #     'Weld_Col_Tr2&4_S6': Weld_Col,
    #     'Total_Col_Tr2&4_S6': Total_Col,
    #     'Trig_Force_Tr2&4_S6': TrigForce,
    #     'Weld_Force_Tr2&4_S6': Weld_Force,
    #     'Freq_Chg_Tr2&4_S6': Freq_Chg,
    #     'Set_AMP_A_Tr2&4_S6': Set_AMP_A,
    #     'Velocity_Tr2&4_S6': Velocity
    # }
         

    # for name, value in data_dict_Tr2vs4_S6.items():
    #     store_and_pubish_data(count, name, name, value, 'UltraSonic Machine')

    # Print the extracted components
    print(f'Weld_Cycle: {Cycle}')
    print(f'Timestamp: {Timestamp}')
    print(f'Date: {Date}')
    print(f'RunTime: {RunTime} s')
    print(f'Pk_Pwr: {Pk_Pwr} %')
    print(f'Energy: {Energy} J')
    print(f'Weld_Abs: {Weld_Abs} mm')
    print(f'Weld_Col: {Weld_Col} mm')
    print(f'Total_Col: {Total_Col} mm')
    print(f'TrigForce: {TrigForce} N')
    print(f'Weld_Force: {Weld_Force} N')
    print(f'Freq_Chg: {Freq_Chg} Hz')
    print(f'Set_AMP_A: {Set_AMP_A} %')
    print(f'Velocity: {Velocity} mm/s')
    print('\n')

    # Store the extracted components in a file
    with lock:
        with open('stored_serial.csv', 'a') as file:
            file.write(f'{parts}\n')
    #     with open('stored_serial.csv', 'a') as file:
    #         file.write(f'{Cycle},{Timestamp},{Date},{RunTime},{Pk_Pwr},{Energy},{Weld_Abs},{Weld_Col}, {Total_Col}, {TrigForce}, {Freq_Chg}, {Set_AMP_A}, {Velocity}\n')


def task_read_serial():
    count = 0
    try:
        while True:
            # Read data until newline character is encountered
            buffer = ""
            while True:
                byte = ser.read(1).decode('utf-8')  # Read a byte and decode it to string
                if byte == '\r':  # Check for carriage return
                    continue  # Skip carriage return
                elif byte == '\n':  # Check for newline character
                    count += 1
                    analyze_string(buffer, count)  # Analyze the received string
                    break  # Break the loop
                else:
                    buffer += byte  # Append the byte to buffer

    except serial.SerialException as e:
        print("Error:", e)

    finally:
        if ser.is_open:
            ser.close()
            print("Serial port closed")
#---------------------------------------------------------------------------

if __name__ == '__main__':
    
    t7 = threading.Thread(target=task_read_serial)

    t7.start()
    t7.join()

        