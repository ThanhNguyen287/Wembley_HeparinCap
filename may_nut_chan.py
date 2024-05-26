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
class ST():
    On = 0
    Run = 1
    Idle = 2
    Alarm = 3
    Setup = 4
    Off = 5
    Ready = 6
    Wifi_disconnect = 7

lock = threading.Lock()

# Tạo các ngắt với các task khi bị mất kết nối ethernet, luồng 11 đến 30 dành cho Input và Output của từng station
reConEth_flg = threading.Event()
enStoreData_flg = threading.Event()
t1_interupt = threading.Event()
t2_interupt = threading.Event()
t3_interupt = threading.Event()
t4_interupt = threading.Event()
t5_interupt = threading.Event()
t6_interupt = threading.Event()
t7_interupt = threading.Event()
t8_interupt = threading.Event()
t9_interupt = threading.Event()
t10_interupt = threading.Event()
t11_interupt = threading.Event()
t12_interupt = threading.Event()
t13_interupt = threading.Event()
t14_interupt = threading.Event()
t15_interupt = threading.Event()
t16_interupt = threading.Event()
t17_interupt = threading.Event()
t18_interupt = threading.Event()
t19_interupt = threading.Event()
t20_interupt = threading.Event()
t21_interupt = threading.Event()
t22_interupt = threading.Event()
t23_interupt = threading.Event()
t24_interupt = threading.Event()
t25_interupt = threading.Event()
t26_interupt = threading.Event()
t27_interupt = threading.Event()
t28_interupt = threading.Event()
t29_interupt = threading.Event()
t30_interupt = threading.Event()
t31_interupt = threading.Event() #task lấy 4 it check pressure S10

t1_interupt.set()
t2_interupt.set()
t3_interupt.set()
t4_interupt.set()
t5_interupt.set()
t6_interupt.set()
t7_interupt.set()
t8_interupt.set()
t9_interupt.set()
# t31_interupt.set() #Chạy chung với S10
# t10_interupt.set() #Chưa cần set vì đợi value trả về từ MQTT trước
# t11_interupt.set() #Chưa cần set vì đợi value trả về từ MQTT trước
# t12_interupt.set() #Chưa cần set vì đợi value trả về từ MQTT trước

# Các biến setting về chiều cao để xác định sản phẩm đạt, ko đạt, chiều cao hiện tại
may_nut_chan_setting_value = pd.read_csv(header_topic_mqtt + 'pi/setting_value.csv', index_col=0)
dset_addr = [*may_nut_chan_setting_value['ID_Setting']]
dset_type = [*may_nut_chan_setting_value['Setting_Type']]
dset_name = [name.strip() for name in [*may_nut_chan_setting_value['Setting_Name']]]
dset_index = [*may_nut_chan_setting_value['Setting_Index']]
dset_lenght = len(dset_addr)
# dset_old = [*may_nut_chan_setting_value['Setting_Value']]
dset_old = [-1]*dset_lenght

# Các biến good, bad, eff 
may_nut_chan_counting_value = pd.read_csv(header_topic_mqtt + 'pi/counting_value.csv', index_col=0)
dcount_addr = [*may_nut_chan_counting_value['ID_Counting']]
dcount_type = [*may_nut_chan_counting_value['Counting_Type']]
dcount_name = [name.strip() for name in [*may_nut_chan_counting_value['Counting_Name']]]
dcount_index = [*may_nut_chan_counting_value['Counting_Index']]
dcount_lenght = len(dcount_addr)
# dcount_old = [*may_nut_chan_counting_value['Counting_Value']]
dcount_old = [-1]*dcount_lenght

# Các biến trang Rejection Details
may_nut_chan_checking_value = pd.read_csv(header_topic_mqtt + 'pi/checking_value.csv', index_col=0)
dcheck_addr = [*may_nut_chan_checking_value['ID_Checking']]
dcheck_type = [*may_nut_chan_checking_value['Checking_Type']]
dcheck_name = [name.strip() for name in [*may_nut_chan_checking_value['Checking_Name']]]
dcheck_index = [*may_nut_chan_checking_value['Checking_Index']]
dcheck_lenght = len(dcheck_addr)
# dcheck_old = [*may_nut_chan_checking_value['Checking_Value']]
dcheck_old = [-1]*dcheck_lenght


# Các biến alarm về danh sách lỗi hiển thị trên HMI
may_nut_chan_alarm_value = pd.read_csv(header_topic_mqtt + 'pi/alarm_value.csv', index_col=0)
dalarm_addr = [*may_nut_chan_alarm_value['ID_Alarm']]
dalarm_type = [*may_nut_chan_alarm_value['Alarm_Type']]
dalarm_name = [name.strip() for name in [*may_nut_chan_alarm_value['Alarm_Name']]]
dalarm_index = [*may_nut_chan_alarm_value['Alarm_Index']]
dalarm_lenght = len(dalarm_addr)
# dalarm_old = [*may_nut_chan_alarm_value['Alarm_Value']]
dalarm_old = [-1]*dalarm_lenght

# Các biến chu kỳ hoạt động của từng station
may_nut_chan_cycle_value = pd.read_csv(header_topic_mqtt + 'pi/cycle_value.csv', index_col=0)
dcycle_addr = [*may_nut_chan_cycle_value['ID_Cycle']]
dcycle_type = [*may_nut_chan_cycle_value['Cycle_Type']]
dcycle_name = [name.strip() for name in [*may_nut_chan_cycle_value['Cycle_Name']]]
dcycle_index = [*may_nut_chan_cycle_value['Cycle_Index']]
dcycle_lenght = len(dcycle_addr)
# dcycle_old = [*may_nut_chan_cycle_value['Cycle_Value']]
dcycle_old = [-1]*dcycle_lenght

# Các biến Input của S1 station
may_nut_chan_input_value1 = pd.read_csv(header_topic_mqtt + 'pi/input_value_S1.csv', index_col=0)
dinput_addr1 = [*may_nut_chan_input_value1['ID_Input']]
dinput_type1 = [*may_nut_chan_input_value1['Input_Type']]
dinput_name1 = [name.strip() for name in [*may_nut_chan_input_value1['Input_Name']]]
dinput_index1 = [*may_nut_chan_input_value1['Input_Index']]
dinput_lenght1 = len(dinput_addr1)
dinput_old1 = [-1]*dinput_lenght1

# Các biến Input của S2 station
may_nut_chan_input_value2 = pd.read_csv(header_topic_mqtt + 'pi/input_value_S2.csv', index_col=0)
dinput_addr2 = [*may_nut_chan_input_value2['ID_Input']]
dinput_type2 = [*may_nut_chan_input_value2['Input_Type']]
dinput_name2 = [name.strip() for name in [*may_nut_chan_input_value2['Input_Name']]]
dinput_index2 = [*may_nut_chan_input_value2['Input_Index']]
dinput_lenght2 = len(dinput_addr2)
dinput_old2 = [-1]*dinput_lenght2

# Các biến Input của S3 station
may_nut_chan_input_value3 = pd.read_csv(header_topic_mqtt + 'pi/input_value_S3.csv', index_col=0)
dinput_addr3 = [*may_nut_chan_input_value3['ID_Input']]
dinput_type3 = [*may_nut_chan_input_value3['Input_Type']]
dinput_name3 = [name.strip() for name in [*may_nut_chan_input_value3['Input_Name']]]
dinput_index3 = [*may_nut_chan_input_value3['Input_Index']]
dinput_lenght3 = len(dinput_addr3)
dinput_old3 = [-1]*dinput_lenght3

# Các biến Input của S4 station
may_nut_chan_input_value4 = pd.read_csv(header_topic_mqtt + 'pi/input_value_S4.csv', index_col=0)
dinput_addr4 = [*may_nut_chan_input_value4['ID_Input']]
dinput_type4 = [*may_nut_chan_input_value4['Input_Type']]
dinput_name4 = [name.strip() for name in [*may_nut_chan_input_value4['Input_Name']]]
dinput_index4 = [*may_nut_chan_input_value4['Input_Index']]
dinput_lenght4 = len(dinput_addr4)
dinput_old4 = [-1]*dinput_lenght4

# Các biến Input của S5 station
may_nut_chan_input_value5 = pd.read_csv(header_topic_mqtt + 'pi/input_value_S5.csv', index_col=0)
dinput_addr5 = [*may_nut_chan_input_value5['ID_Input']]
dinput_type5 = [*may_nut_chan_input_value5['Input_Type']]
dinput_name5 = [name.strip() for name in [*may_nut_chan_input_value5['Input_Name']]]
dinput_index5 = [*may_nut_chan_input_value5['Input_Index']]
dinput_lenght5 = len(dinput_addr5)
dinput_old5 = [-1]*dinput_lenght5

# Các biến Input của S6 station
may_nut_chan_input_value6 = pd.read_csv(header_topic_mqtt + 'pi/input_value_S6.csv', index_col=0)
dinput_addr6 = [*may_nut_chan_input_value6['ID_Input']]
dinput_type6 = [*may_nut_chan_input_value6['Input_Type']]
dinput_name6 = [name.strip() for name in [*may_nut_chan_input_value6['Input_Name']]]
dinput_index6 = [*may_nut_chan_input_value6['Input_Index']]
dinput_lenght6 = len(dinput_addr6)
dinput_old6 = [-1]*dinput_lenght6

# Các biến Input của S7 station
may_nut_chan_input_value7 = pd.read_csv(header_topic_mqtt + 'pi/input_value_S7.csv', index_col=0)
dinput_addr7 = [*may_nut_chan_input_value7['ID_Input']]
dinput_type7 = [*may_nut_chan_input_value7['Input_Type']]
dinput_name7 = [name.strip() for name in [*may_nut_chan_input_value7['Input_Name']]]
dinput_index7 = [*may_nut_chan_input_value7['Input_Index']]
dinput_lenght7 = len(dinput_addr7)
dinput_old7 = [-1]*dinput_lenght7

# Các biến Input của S10 station
may_nut_chan_input_value10 = pd.read_csv(header_topic_mqtt + 'pi/input_value_S10.csv', index_col=0)
dinput_addr10 = [*may_nut_chan_input_value10['ID_Input']]
dinput_type10 = [*may_nut_chan_input_value10['Input_Type']]
dinput_name10 = [name.strip() for name in [*may_nut_chan_input_value10['Input_Name']]]
dinput_index10 = [*may_nut_chan_input_value10['Input_Index']]
dinput_lenght10 = len(dinput_addr10)
dinput_old10 = [-1]*dinput_lenght10

# Các biến Input của S11 station
may_nut_chan_input_value11 = pd.read_csv(header_topic_mqtt + 'pi/input_value_S11.csv', index_col=0)
dinput_addr11 = [*may_nut_chan_input_value11['ID_Input']]
dinput_type11 = [*may_nut_chan_input_value11['Input_Type']]
dinput_name11 = [name.strip() for name in [*may_nut_chan_input_value11['Input_Name']]]
dinput_index11 = [*may_nut_chan_input_value11['Input_Index']]
dinput_lenght11 = len(dinput_addr11)
dinput_old11 = [-1]*dinput_lenght11

# Các biến Input của S12 station
may_nut_chan_input_value12 = pd.read_csv(header_topic_mqtt + 'pi/input_value_S12.csv', index_col=0)
dinput_addr12 = [*may_nut_chan_input_value12['ID_Input']]
dinput_type12 = [*may_nut_chan_input_value12['Input_Type']]
dinput_name12 = [name.strip() for name in [*may_nut_chan_input_value12['Input_Name']]]
dinput_index12 = [*may_nut_chan_input_value12['Input_Index']]
dinput_lenght12 = len(dinput_addr12)
dinput_old12 = [-1]*dinput_lenght12

# Các biến Output của S1 station
may_nut_chan_output_value1 = pd.read_csv(header_topic_mqtt + 'pi/output_value_S1.csv', index_col=0)
doutput_addr1 = [*may_nut_chan_output_value1['ID_Output']]
doutput_type1 = [*may_nut_chan_output_value1['Output_Type']]
doutput_name1 = [name.strip() for name in [*may_nut_chan_output_value1['Output_Name']]]
doutput_index1 = [*may_nut_chan_output_value1['Output_Index']]
doutput_lenght1 = len(doutput_addr1)
doutput_old1 = [-1]*doutput_lenght1

# Các biến Output của S2 station
may_nut_chan_output_value2 = pd.read_csv(header_topic_mqtt + 'pi/output_value_S2.csv', index_col=0)
doutput_addr2 = [*may_nut_chan_output_value2['ID_Output']]
doutput_type2 = [*may_nut_chan_output_value2['Output_Type']]
doutput_name2 = [name.strip() for name in [*may_nut_chan_output_value2['Output_Name']]]
doutput_index2 = [*may_nut_chan_output_value2['Output_Index']]
doutput_lenght2 = len(doutput_addr2)
doutput_old2 = [-1]*doutput_lenght2

# Các biến Output của S3 station
may_nut_chan_output_value3 = pd.read_csv(header_topic_mqtt + 'pi/output_value_S3.csv', index_col=0)
doutput_addr3 = [*may_nut_chan_output_value3['ID_Output']]
doutput_type3 = [*may_nut_chan_output_value3['Output_Type']]
doutput_name3 = [name.strip() for name in [*may_nut_chan_output_value3['Output_Name']]]
doutput_index3 = [*may_nut_chan_output_value3['Output_Index']]
doutput_lenght3 = len(doutput_addr3)
doutput_old3 = [-1]*doutput_lenght3

# Các biến Output của S4 station
may_nut_chan_output_value4 = pd.read_csv(header_topic_mqtt + 'pi/output_value_S4.csv', index_col=0)
doutput_addr4 = [*may_nut_chan_output_value4['ID_Output']]
doutput_type4 = [*may_nut_chan_output_value4['Output_Type']]
doutput_name4 = [name.strip() for name in [*may_nut_chan_output_value4['Output_Name']]]
doutput_index4 = [*may_nut_chan_output_value4['Output_Index']]
doutput_lenght4 = len(doutput_addr4)
doutput_old4 = [-1]*doutput_lenght4

# Các biến Output của S5 station
may_nut_chan_output_value5 = pd.read_csv(header_topic_mqtt + 'pi/output_value_S5.csv', index_col=0)
doutput_addr5 = [*may_nut_chan_output_value5['ID_Output']]
doutput_type5 = [*may_nut_chan_output_value5['Output_Type']]
doutput_name5 = [name.strip() for name in [*may_nut_chan_output_value5['Output_Name']]]
doutput_index5 = [*may_nut_chan_output_value5['Output_Index']]
doutput_lenght5 = len(doutput_addr5)
doutput_old5 = [-1]*doutput_lenght5

# Các biến Output của S6 station
may_nut_chan_output_value6 = pd.read_csv(header_topic_mqtt + 'pi/output_value_S6.csv', index_col=0)
doutput_addr6 = [*may_nut_chan_output_value6['ID_Output']]
doutput_type6 = [*may_nut_chan_output_value6['Output_Type']]
doutput_name6 = [name.strip() for name in [*may_nut_chan_output_value6['Output_Name']]]
doutput_index6 = [*may_nut_chan_output_value6['Output_Index']]
doutput_lenght6 = len(doutput_addr6)
doutput_old6 = [-1]*doutput_lenght6

# Các biến Output của S7 station
may_nut_chan_output_value7 = pd.read_csv(header_topic_mqtt + 'pi/output_value_S7.csv', index_col=0)
doutput_addr7 = [*may_nut_chan_output_value7['ID_Output']]
doutput_type7 = [*may_nut_chan_output_value7['Output_Type']]
doutput_name7 = [name.strip() for name in [*may_nut_chan_output_value7['Output_Name']]]
doutput_index7 = [*may_nut_chan_output_value7['Output_Index']]
doutput_lenght7 = len(doutput_addr7)
doutput_old7 = [-1]*doutput_lenght7

# Các biến Output của S10 station
may_nut_chan_output_value10 = pd.read_csv(header_topic_mqtt + 'pi/output_value_S10.csv', index_col=0)
doutput_addr10 = [*may_nut_chan_output_value10['ID_Output']]
doutput_type10 = [*may_nut_chan_output_value10['Output_Type']]
doutput_name10 = [name.strip() for name in [*may_nut_chan_output_value10['Output_Name']]]
doutput_index10 = [*may_nut_chan_output_value10['Output_Index']]
doutput_lenght10 = len(doutput_addr10)
doutput_old10 = [-1]*doutput_lenght10

# Các biến Output của S11 station
may_nut_chan_output_value11 = pd.read_csv(header_topic_mqtt + 'pi/output_value_S11.csv', index_col=0)
doutput_addr11 = [*may_nut_chan_output_value11['ID_Output']]
doutput_type11 = [*may_nut_chan_output_value11['Output_Type']]
doutput_name11 = [name.strip() for name in [*may_nut_chan_output_value11['Output_Name']]]
doutput_index11 = [*may_nut_chan_output_value11['Output_Index']]
doutput_lenght11 = len(doutput_addr11)
doutput_old11 = [-1]*doutput_lenght11

# Các biến Output của S12 station
may_nut_chan_output_value12 = pd.read_csv(header_topic_mqtt + 'pi/output_value_S12.csv', index_col=0)
doutput_addr12 = [*may_nut_chan_output_value12['ID_Output']]
doutput_type12 = [*may_nut_chan_output_value12['Output_Type']]
doutput_name12 = [name.strip() for name in [*may_nut_chan_output_value12['Output_Name']]]
doutput_index12 = [*may_nut_chan_output_value12['Output_Index']]
doutput_lenght12 = len(doutput_addr12)
doutput_old12 = [-1]*doutput_lenght12

# Các biến Output của Check Pressure S10 station
may_nut_chan_pressure_value = pd.read_csv(header_topic_mqtt + 'pi/check_pressureS10_value.csv', index_col=0)
dpressure_addr = [*may_nut_chan_pressure_value['ID_Pressure']]
dpressure_type = [*may_nut_chan_pressure_value['Pressure_Type']]
dpressure_name = [name.strip() for name in [*may_nut_chan_pressure_value['Pressure_Name']]]
dpressure_index = [*may_nut_chan_pressure_value['Pressure_Index']]
dpressure_lenght = len(dpressure_addr)
dpressure_old = [-1]*dpressure_lenght

# Các tên biến cho Desktop
variable_name_desktop = pd.read_csv(header_topic_mqtt + 'pi/variable_name_desktop.csv', index_col=0)
desktop_var_name = [name.strip() for name in [*variable_name_desktop['Desktop_Name']]]


is_connectWifi = 0
status_old = -1
initRunSt = 1
runStTimestamp = None
onStTimestamp = None
'''
M1	    MANUAL MODE
M0	    AUTO ON
M170	ALL B/F FLT
M3	    FLT FLG
Y4	    INDEX MOTOR ON
T4	    M/C IDLE TD
M799    POWER ON

M0, Y4  	Run	    -->     M0.Y4./M170./M3./T4
M170, M3	Alarm	-->     (M170+M3)
T4	        Idle	-->     T4./Y4./M170./M3
M1, Y4  	Setup   -->     M1.Y4./M170./M3./T4
Y4 = 0	    Ready	-->     /Y4./T4./M170./M3
M799 = 0    Off     -->     /M799 hoặc khi code bị lỗi do ko thể giao tiếp với PLC        
M799 = 1    ON      -->     M799 

'''
list_status_addr = ['M1', 'M0', 'Y4', 'M3', 'TS4', 'SM400']
list_status_old = [0]*len(list_status_addr)

#---------------------------------------------------------------------------
def create_excel_file(file_name):
    try:
        with open(header_topic_mqtt + 'pi/' + file_name) as store_data:
            pass
    except FileNotFoundError:
        with open(header_topic_mqtt + 'pi/' + file_name,'w+') as store_data:
            store_data.write('{0},{1},{2},{3},{4},{5},{6}\n'.format('No.','ID','Name Variable','Value','Timestamp','Kind of Data','Connected Wifi'))
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

#----------------------Option Functions-----------------------
def restart_program():
    python = sys.executable
    os.execl(python,python, *sys.argv)


def restart_raspberry():
    os.system('sudo reboot')

#-------------------------------------------------------------

#topic_standard = 'HCM/IE-F2-HCA01/Metric/'
#topic_standard = 'Test/WEMBLEY/Data/'
topic_desktop_backend = 'WembleyMedical/HCM/IE-F2-HCA01/Backend/'
topic_desktop_app = 'WembleyMedical/HCM/IE-F2-HCA01/Desktop/'
topic_standard = 'Wembley/HerapinCap/IE-F2-HCA01/'

#----------------Store and Publish Functions------------------
def store_and_pubish_data(No, Name, Device, RealValue, KindOfData):
    global is_connectWifi, topic_desktop_app, topic_desktop_backend
    with lock:
        Timestamp = datetime.now().isoformat(timespec='microseconds')
        with open(header_topic_mqtt + 'pi/stored_data.csv','a+') as store_data:
            store_data.write('{0},{1},{2},{3},{4},{5},{6}\n'.format(No, Device, Name, RealValue, Timestamp, KindOfData, is_connectWifi))

        data = generate_data(Name, RealValue)
        mqtt_topic = topic_standard + str(Name)
        if KindOfData == 'Alarm':
            data = generate_data(Device, RealValue)
        print(data)
        client.publish(mqtt_topic,str(data),1,1)

        # Publish data for Desktop App
        client.publish(topic_desktop_backend + str(Name),str(data),1,1)
        if Name in desktop_var_name:
            client.publish(topic_desktop_app + str(Name),str(data),1,1)
        #------------------------------

        if is_connectWifi:
            # print(f'{No}: ',Device, Name, RealValue)
            pass
        else:
            print('LOG ->', f'{No}: ',Device, Name, RealValue)
            with open(header_topic_mqtt + 'pi/stored_disconnectWifi_data.txt', 'a+') as file:
                file.write(str(data)+'\n')


def store_and_publish_status(No, nameStatus, IDStatus):
    global is_connectWifi, topic_desktop_app, topic_desktop_backend
    with lock:
        Timestamp = datetime.now().isoformat(timespec='microseconds')

        with open(header_topic_mqtt + 'pi/stored_data.csv','a+') as store_data:
            store_data.write('{0},{1},{2},{3},{4},{5},{6}\n'.format(No, nameStatus, nameStatus, IDStatus, Timestamp, 'MachineStatus', is_connectWifi))

        data = str(generate_data_status(nameStatus, IDStatus))
        client.publish(topic_standard + 'machineStatus',data,1,1)

        # Publish data for Desktop App
        client.publish(topic_desktop_backend + 'machineStatus',data,1,1)
        client.publish(topic_desktop_app + 'machineStatus',data,1,1)
        #------------------------------

        print(data)

        if not is_connectWifi:
            with open(header_topic_mqtt + 'pi/stored_disconnectWifi_data.txt', 'a+') as file:
                file.write(data+'\n')

#-------------------------------------------------------------

# --------------------------- Setup MQTT -------------------------------------
# Define MQTT call-back function
def on_connect(client, userdata, flags, rc):
    global status_old, is_connectWifi, initRunSt, onStTimestamp, runStTimestamp
    print('Connected to MQTT broker with result code ' + str(rc))

    if status_old == 0:
        client.publish(topic_standard + 'machineStatus',str(generate_data_status('On', 0)),1,1)
        client.publish(topic_desktop_app + 'machineStatus',str(generate_data_status('On', 0)),1,1)
        client.publish(topic_desktop_backend + 'machineStatus',str(generate_data_status('On', 0)),1,1)
    elif status_old == 1:
        client.publish(topic_standard + 'machineStatus',str(generate_data_status('Run', 1)),1,1)
        client.publish(topic_desktop_app + 'machineStatus',str(generate_data_status('Run', 1)),1,1)
        client.publish(topic_desktop_backend + 'machineStatus',str(generate_data_status('Run', 1)),1,1)
    elif status_old == 2:
        client.publish(topic_standard + 'machineStatus',str(generate_data_status('Idle', 2)),1,1)
        client.publish(topic_desktop_app + 'machineStatus',str(generate_data_status('Idle', 2)),1,1)
        client.publish(topic_desktop_backend + 'machineStatus',str(generate_data_status('Idle', 2)),1,1)
    elif status_old == 3:
        client.publish(topic_standard + 'machineStatus',str(generate_data_status('Alarm', 3)),1,1)
        client.publish(topic_desktop_app + 'machineStatus',str(generate_data_status('Alarm', 3)),1,1)
        client.publish(topic_desktop_backend + 'machineStatus',str(generate_data_status('Alarm', 3)),1,1)
    elif status_old == 4:
        client.publish(topic_standard + 'machineStatus',str(generate_data_status('Setup', 4)),1,1)
        client.publish(topic_desktop_app + 'machineStatus',str(generate_data_status('Setup', 4)),1,1)
        client.publish(topic_desktop_backend + 'machineStatus',str(generate_data_status('Setup', 4)),1,1)
    elif status_old == 5:
        client.publish(topic_standard + 'machineStatus',str(generate_data_status('OFF', 5)),1,1)
        client.publish(topic_desktop_app + 'machineStatus',str(generate_data_status('OFF', 5)),1,1)
        client.publish(topic_desktop_backend + 'machineStatus',str(generate_data_status('OFF', 5)),1,1)
    elif status_old == 6:
        client.publish(topic_standard + 'machineStatus',str(generate_data_status('Ready', 6)),1,1)
        client.publish(topic_desktop_app + 'machineStatus',str(generate_data_status('Ready', 6)),1,1)
        client.publish(topic_desktop_backend + 'machineStatus',str(generate_data_status('Ready', 6)),1,1)
    elif status_old == 7:
        client.publish(topic_standard + 'machineStatus',str(generate_data_status('Wifi disconnect', 7)),1,1)
        client.publish(topic_desktop_app + 'machineStatus',str(generate_data_status('Wifi disconnect', 7)),1,1)
        client.publish(topic_desktop_backend + 'machineStatus',str(generate_data_status('Wifi disconnect', 7)),1,1)

    if onStTimestamp != None:
        client.publish(topic_standard + 'machineStatus', str(onStTimestamp), 1, 1)
        print(onStTimestamp)
        onStTimestamp = None
        
    if runStTimestamp != None:
        client.publish(topic_standard + 'machineStatus', str(runStTimestamp), 1, 1)
        print(runStTimestamp)
        runStTimestamp = None
    
    initRunSt = 0
    is_connectWifi = 1

def on_disconnect(client, userdata, rc):
    global is_connectWifi
    if rc != 0:
        print('Unexpected disconnection from MQTT broker')
        is_connectWifi = 0


mqttBroker = '52.141.29.70'  # cloud
# mqttBroker = '10.0.70.45'  # cloud
mqttPort = 1883
mqttKeepAliveINTERVAL = 45

# Initiate Mqtt Client
client = mqtt.Client()
# if machine is immediately turned off --> last_will sends 'machineStatus: Off' to topic
client.will_set(topic_standard + 'machineStatus',str(generate_data_status('Off', ST.Off)),1,1)
# Register callback function
client.on_connect = on_connect
client.on_disconnect = on_disconnect
# Connect with MQTT Broker
print('connecting to broker ',mqttBroker)
# Check connection to MQTT Broker 
try:
	client.connect(mqttBroker, mqttPort, mqttKeepAliveINTERVAL)
except:
	print("Can't connect MQTT Broker!")
	
client.loop_start()
time.sleep(1)

# client 2
mqttBroker = '52.141.29.70'  # cloud
# mqttBroker = '10.0.70.45'  # cloud
mqttPort = 1883
mqttKeepAliveINTERVAL = 45

# Initiate Mqtt Client
client2 = mqtt.Client()
# if machine is immediately turned off --> last_will sends 'machineStatus: Off' to topic
client2.will_set(topic_desktop_app + 'machineStatus',str(generate_data_status('Off', ST.Off)),1,1)
# Register callback function
# client2.on_connect = on_connect
# client2.on_disconnect = on_disconnect
# Connect with MQTT Broker
print('connecting to broker ',mqttBroker)
# Check connection to MQTT Broker 
try:
	client2.connect(mqttBroker, mqttPort, mqttKeepAliveINTERVAL)
except:
	print("Can't connect MQTT Broker!")
	
client2.loop_start()
time.sleep(1)

# client 3
mqttBroker = '52.141.29.70'  # cloud
# mqttBroker = '10.0.70.45'  # cloud
mqttPort = 1883
mqttKeepAliveINTERVAL = 45

# Initiate Mqtt Client
client3 = mqtt.Client()
# if machine is immediately turned off --> last_will sends 'machineStatus: Off' to topic
#client3.will_set(topic_desktop_backend + 'machineStatus',str(generate_data_status('Off', ST.Off)),1,1)
# Register callback function
# client3.on_connect = on_connect
# client3.on_disconnect = on_disconnect
# Connect with MQTT Broker
print('connecting to broker ',mqttBroker)
# Check connection to MQTT Broker 
try:
	client3.connect(mqttBroker, mqttPort, mqttKeepAliveINTERVAL)
except:
	print("Can't connect MQTT Broker!")
	
client3.loop_start()
time.sleep(1)
#----------------------------Setup MQTT Subcribe---------------------------------
topic_subcribe='Wembley/HerapinCap/IE-F2-HCA01/EnableStation'
'''
    Quy định trạng thái:
    0: Tắt hết các station và Encoder
    1: S1
    2: S2
    3. S3
    4. S4
    5. S5
    6. S6
    7. S7
    10. S10
    11. S11
    12. S12
    13. Encoder

'''
# client 4
mqttBroker = '52.141.29.70'  # cloud
# mqttBroker = '10.0.70.45'  # cloud
mqttPort = 1883
mqttKeepAliveINTERVAL = 45
# Initiate Mqtt Client Station
client4 = mqtt.Client()
# Register callback function
client4.on_connect = on_connect
client4.on_disconnect = on_disconnect
try:
	client4.connect(mqttBroker, mqttPort, mqttKeepAliveINTERVAL)
except:
	print("Can't connect MQTT Broker!")
client4.loop_start()
time.sleep(1)
# Giá trị mặc định cho value_received
value_received = 0  # Khởi tạo để tránh NameError
old_value_received = 0
def on_message(client, userdata, message):
    global value_received, old_value_received
    #print("message received  " ,str(message.payload.decode("utf-8")))
    value_received = int(message.payload.decode("utf-8"))
    print(value_received)
    #print(type(value_received))
    if value_received == 1: # Kích hoạt luồng S1
        t11_interupt.set()
        t12_interupt.set()
        # t10_interupt.clear()
        # t13_interupt.clear()
        # t14_interupt.clear()
        # t15_interupt.clear()
        # t16_interupt.clear()
        # t17_interupt.clear()
        # t18_interupt.clear()
        # t19_interupt.clear()
        # t20_interupt.clear()
        # t21_interupt.clear()
        # t22_interupt.clear()
        # t23_interupt.clear()
        # t24_interupt.clear()
        # t25_interupt.clear()
        # t26_interupt.clear()
        # t27_interupt.clear()
        # t28_interupt.clear()
        # t29_interupt.clear()
        # t30_interupt.clear()
        # t31_interupt.clear()
    elif value_received == 2:
        t13_interupt.set() # Kích hoạt luồng S2
        t14_interupt.set()
        # t10_interupt.clear()
        # t11_interupt.clear()
        # t12_interupt.clear()
        # t15_interupt.clear()
        # t16_interupt.clear()
        # t17_interupt.clear()
        # t18_interupt.clear()
        # t19_interupt.clear()
        # t20_interupt.clear()
        # t21_interupt.clear()
        # t22_interupt.clear()
        # t23_interupt.clear()
        # t24_interupt.clear()
        # t25_interupt.clear()
        # t26_interupt.clear()
        # t27_interupt.clear()
        # t28_interupt.clear()
        # t29_interupt.clear()
        # t30_interupt.clear()
        # t31_interupt.clear()
    elif value_received == 3: # Kích hoạt luồng S3
        t15_interupt.set()
        t16_interupt.set()
        # t10_interupt.clear()
        # t11_interupt.clear()
        # t12_interupt.clear()
        # t13_interupt.clear()
        # t14_interupt.clear()
        # t17_interupt.clear()
        # t18_interupt.clear()
        # t19_interupt.clear()
        # t20_interupt.clear()
        # t21_interupt.clear()
        # t22_interupt.clear()
        # t23_interupt.clear()
        # t24_interupt.clear()
        # t25_interupt.clear()
        # t26_interupt.clear()
        # t27_interupt.clear()
        # t28_interupt.clear()
        # t29_interupt.clear()
        # t30_interupt.clear()
        # t31_interupt.clear()
    elif value_received == 4: # Kích hoạt luồng S4
        t17_interupt.set()
        t18_interupt.set()
        # t10_interupt.clear()
        # t11_interupt.clear()
        # t12_interupt.clear()
        # t13_interupt.clear()
        # t14_interupt.clear()
        # t15_interupt.clear()
        # t16_interupt.clear()
        # t19_interupt.clear()
        # t20_interupt.clear()
        # t21_interupt.clear()
        # t22_interupt.clear()
        # t23_interupt.clear()
        # t24_interupt.clear()
        # t25_interupt.clear()
        # t26_interupt.clear()
        # t27_interupt.clear()
        # t28_interupt.clear()
        # t29_interupt.clear()
        # t30_interupt.clear()
        # t31_interupt.clear()
    elif value_received == 5: # Kích hoạt luồng S5
        t19_interupt.set()
        t20_interupt.set()
        # t10_interupt.clear()
        # t11_interupt.clear()
        # t12_interupt.clear()
        # t13_interupt.clear()
        # t14_interupt.clear()
        # t15_interupt.clear()
        # t16_interupt.clear()
        # t17_interupt.clear()
        # t18_interupt.clear()
        # t21_interupt.clear()
        # t22_interupt.clear()
        # t23_interupt.clear()
        # t24_interupt.clear()
        # t25_interupt.clear()
        # t26_interupt.clear()
        # t27_interupt.clear()
        # t28_interupt.clear()
        # t29_interupt.clear()
        # t30_interupt.clear()
        # t31_interupt.clear()
    elif value_received == 6: # Kích hoạt luồng S6
        t21_interupt.set()
        t22_interupt.set()
        # t10_interupt.clear()
        # t11_interupt.clear()
        # t12_interupt.clear()
        # t13_interupt.clear()
        # t14_interupt.clear()
        # t15_interupt.clear()
        # t16_interupt.clear()
        # t17_interupt.clear()
        # t18_interupt.clear()
        # t19_interupt.clear()
        # t20_interupt.clear()
        # t23_interupt.clear()
        # t24_interupt.clear()
        # t25_interupt.clear()
        # t26_interupt.clear()
        # t27_interupt.clear()
        # t28_interupt.clear()
        # t29_interupt.clear()
        # t30_interupt.clear()
        # t31_interupt.clear()
    elif value_received == 7: # Kích hoạt luồng S7
        t23_interupt.set()
        t24_interupt.set()
        # t10_interupt.clear()
        # t11_interupt.clear()
        # t12_interupt.clear()
        # t13_interupt.clear()
        # t14_interupt.clear()
        # t15_interupt.clear()
        # t16_interupt.clear()
        # t17_interupt.clear()
        # t18_interupt.clear()
        # t19_interupt.clear()
        # t20_interupt.clear()
        # t21_interupt.clear()
        # t22_interupt.clear()
        # t25_interupt.clear()
        # t26_interupt.clear()
        # t27_interupt.clear()
        # t28_interupt.clear()
        # t29_interupt.clear()
        # t30_interupt.clear()
        # t31_interupt.clear()
    elif value_received == 10: # Kích hoạt luồng S10
        t25_interupt.set()
        t26_interupt.set()
        t31_interupt.set()
        # t10_interupt.clear()
        # t11_interupt.clear()
        # t12_interupt.clear()
        # t13_interupt.clear()
        # t14_interupt.clear()
        # t15_interupt.clear()
        # t16_interupt.clear()
        # t17_interupt.clear()
        # t18_interupt.clear()
        # t19_interupt.clear()
        # t20_interupt.clear()
        # t21_interupt.clear()
        # t22_interupt.clear()
        # t23_interupt.clear()
        # t24_interupt.clear()
        # t27_interupt.clear()
        # t28_interupt.clear()
        # t29_interupt.clear()
        # t30_interupt.clear()
    elif value_received == 11: # Kích hoạt luồng S11
        t27_interupt.set()
        t28_interupt.set()
        # t10_interupt.clear()
        # t11_interupt.clear()
        # t12_interupt.clear()
        # t13_interupt.clear()
        # t14_interupt.clear()
        # t15_interupt.clear()
        # t16_interupt.clear()
        # t17_interupt.clear()
        # t18_interupt.clear()
        # t19_interupt.clear()
        # t20_interupt.clear()
        # t21_interupt.clear()
        # t22_interupt.clear()
        # t23_interupt.clear()
        # t24_interupt.clear()
        # t25_interupt.clear()
        # t29_interupt.clear()
        # t30_interupt.clear()
        # t31_interupt.clear()
    elif value_received == 12: # Kích hoạt luồng S12
        t29_interupt.set()
        t30_interupt.set()
        # t10_interupt.clear()
        # t11_interupt.clear()
        # t12_interupt.clear()
        # t13_interupt.clear()
        # t14_interupt.clear()
        # t15_interupt.clear()
        # t16_interupt.clear()
        # t17_interupt.clear()
        # t18_interupt.clear()
        # t19_interupt.clear()
        # t20_interupt.clear()
        # t21_interupt.clear()
        # t22_interupt.clear()
        # t23_interupt.clear()
        # t24_interupt.clear()
        # t25_interupt.clear()
        # t26_interupt.clear()
        # t27_interupt.clear()
        # t28_interupt.clear()
        # t31_interupt.clear()
    elif value_received == 13: # Kích hoạt luồng Encoder
        #time.sleep(5)
        t10_interupt.set()
        #print(t10_interupt.is_set())
        # t11_interupt.clear()
        # t12_interupt.clear()
        # t13_interupt.clear()
        # t14_interupt.clear()
        # t15_interupt.clear()
        # t16_interupt.clear()
        # t17_interupt.clear()
        # t18_interupt.clear()
        # t19_interupt.clear()
        # t20_interupt.clear()
        # t21_interupt.clear()
        # t22_interupt.clear()
        # t23_interupt.clear()
        # t24_interupt.clear()
        # t25_interupt.clear()
        # t26_interupt.clear()
        # t27_interupt.clear()
        # t28_interupt.clear()
        # t29_interupt.clear()
        # t30_interupt.clear()
        # t31_interupt.clear()
    else:
        #time.sleep(1)
        t10_interupt.clear()
        t11_interupt.clear()
        t12_interupt.clear()
        t13_interupt.clear()
        t14_interupt.clear()
        t15_interupt.clear()
        t16_interupt.clear()
        t17_interupt.clear()
        t18_interupt.clear()
        t19_interupt.clear()
        t20_interupt.clear()
        t21_interupt.clear()
        t22_interupt.clear()
        t23_interupt.clear()
        t24_interupt.clear()
        t25_interupt.clear()
        t26_interupt.clear()
        t27_interupt.clear()
        t28_interupt.clear()
        t29_interupt.clear()
        t30_interupt.clear()
        t31_interupt.clear()
#--------------------------- Setup MQTT SPB ----------------------------
def callback_message_device(topic, payload):
    try:
        print("Received MESSAGE: %s - %s" % (topic, payload))

        for item in payload['metrics']:
            if item['name'] == 'Reboot' and item['value'] == True:
                print('REBOOT!')
                restart_raspberry()
    except Exception as e:
        print(e)

mqttBroker = '52.141.29.70'  # cloud

GroupId = "WB"
NodeId = "NC"
DeviceId = "NCmachine"

_DEBUG = True  # Enable debug messages

Device = MqttSpbEntityDevice(GroupId, NodeId, DeviceId, _DEBUG)

Device.on_message = callback_message_device  # Received messages

Device.data.set_value('machineStatus', 'On')

# Connect to the broker --------------------------------------------
Device.connect(mqttBroker, 1883, "user", "password")
time.sleep(1)
Device.publish_birth()

#----------------------------------------------------------------------

old_operationTime = datetime.now()
offset_operationTime = 0

productCountAddr = 'D3039'
count = 0
while True:
    try:
        read_result = plc.batch_read(ref_device=productCountAddr, read_size=1, data_type=DT.SDWORD)
        # read_result = read_result[0].value
        read_result = 0
        print(f'Product Count Current: {read_result}')
        if int(read_result) > 10:
            print('HMI has not been reset! --> Please Reset to start supervisor operation!')
            time.sleep(1)
        else:
            store_and_publish_status(0, 'On', ST.On)
            status_old = ST.On
            time.sleep(1)
            if not is_connectWifi:
                timestamp = datetime.now().isoformat(timespec='microseconds')
                onStTimestamp = generate_data_disconnectWifi('machineStatus', ST.On, timestamp)
                print(onStTimestamp)
            break
    except Exception as e:
        print(e)
        count += 1
        print(f'Raspberry was disconnected to PLC! --> Please Reset Raspberry again! {count}')
        if count == 10:
            restart_program()
        time.sleep(1)

# ----------------------------------------------------------------------------

#---------------------------------------------------------------------------
def task_data_setting_process():
    count = 0

    # t1_interupt.wait()
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='D4000',
                read_size=500, 
                data_type=DT.SWORD
            )
        for i in range(dset_lenght):
            index = dset_index[i]
            RealValue = int(read_result[index].value)/100
            Name = dset_name[i]
            Device = read_result[index].device
            store_and_pubish_data(count, Name, Device, RealValue, 'Setting')
            dset_old[i] = RealValue
            count+=1

    except Exception as e:
        print('task_data_setting_process')
        print(e)
        reConEth_flg.set()

    while True:
        try:
            time.sleep(0.005)
            with lock:
                read_result = plc.batch_read(
                    ref_device='D4000',
                    read_size=500, 
                    data_type=DT.SWORD
                )
            for i in range(dset_lenght):
                index = dset_index[i]
                RealValue = int(read_result[index].value)/100
                Name = dset_name[i]
                Device = read_result[index].device
                if dset_old[i] != RealValue:
                    store_and_pubish_data(count, Name, Device, RealValue, 'Setting')
                    dset_old[i] = RealValue
                    count+=1

        except Exception as e:
            print('task_data_setting_process')
            print(e)
            reConEth_flg.set()

def task_data_count_process():
    count = 0

    while True:
        try:
            time.sleep(0.005)
            with lock:
                read_result = plc.batch_read(
                    ref_device='D3000',
                    read_size=200, 
                    data_type=DT.UWORD
                )
            for i in range(dcount_lenght):
                index = dcount_index[i]
                if dcount_name[i] == 'EFF':
                    RealValue =  int(read_result[index].value)/10
                else:
                    RealValue = read_result[index].value
                Name = dcount_name[i]
                Device = read_result[index].device
                if dcount_old[i] != RealValue:
                    store_and_pubish_data(count, Name, Device, RealValue, 'Counting')
                    dcount_old[i] = RealValue
                    count+=1

        except Exception as e:
            print('task_data_count_process')
            print(e)
            reConEth_flg.set()    

def task_publish_operationTime():
    global old_operationTime, offset_operationTime, topic_desktop_app
    while True:
        time.sleep(1)
        # t10_interupt.wait()

        new_operationTime = datetime.now()
        delta_operationTime = (new_operationTime - old_operationTime + timedelta(seconds=offset_operationTime)).total_seconds()
        _delta_operationTime = (datetime.fromtimestamp(delta_operationTime) + timedelta(hours=-7)).strftime('%H:%M:%S')
        data = generate_data('operationTimeRaw', _delta_operationTime)
        topic = topic_desktop_app + 'operationTimeRaw'
        with lock:
            client.publish(topic, data, 1, 1)
            # with open(header_topic_mqtt + 'pi/old_operationTime.txt', 'w+') as file:
            #     file.write(str(delta_operationTime))
        print(data)

def task_data_checking_process():
    count = 0

    while True:
        try:
            time.sleep(0.005)
            with lock:
                read_result = plc.batch_read(
                    ref_device='D3000',
                    read_size=100, 
                    data_type=DT.SWORD
                )
            for i in range(dcheck_lenght):
                index = dcheck_index[i]
                RealValue = read_result[index].value
                Name = dcheck_name[i]
                Device = read_result[index].device
                if dcheck_old[i] != RealValue:
                    store_and_pubish_data(count, Name, Device, RealValue, 'Checking')
                    dcheck_old[i] = RealValue
                    count+=1

        except Exception as e:
            print('task_data_checking_process')
            print(e)
            reConEth_flg.set()

def task_data_alarm_process():
    count = 0
    while True:
        try:
            time.sleep(0.005)
            with lock:
                read_result = plc.batch_read(
                    ref_device='M0',
                    read_size=250, 
                    data_type=DT.BIT
                )
            for i in range(dalarm_lenght):
                index = dalarm_index[i]
                RealValue = read_result[index].value
                Name = dalarm_name[i]
                Device = read_result[index].device
                if dalarm_old[i] != RealValue:
                    store_and_pubish_data(count, Name, Device, RealValue, 'Alarm')
                    dalarm_old[i] = RealValue
                    count+=1

        except Exception as e:
            print('task_data_alarm_process')
            print(e)
            reConEth_flg.set()

def task_display_tablePosition():
    t10_interupt.wait()
    old_tablePosition = 0
    count = 0
    while True:
        t10_interupt.wait()
        #print('task_display_tablePosition')
        #if not t10_interupt.is_set():
            #print('t10_interupt')
        #    break
        try: 
            time.sleep(0.001)
            with lock:
                read_result = plc.batch_read(
                    ref_device='D100',
                    read_size=1, 
                    data_type=DT.SWORD, 
                )
            RealValue = read_result[0].value
            Name = 'Encoder Value'
            Device = read_result[0].device
            if old_tablePosition != RealValue:
                store_and_pubish_data(count, Name, Device, RealValue, 'Encoder')
                old_tablePosition = RealValue
                count+=1

        except Exception as e:
            print(e)
            reConEth_flg.set()
        

def task_data_cycle_process():
    count = 0
    while True:
        try:
            time.sleep(0.005)
            with lock:
                read_result = plc.batch_read(
                    ref_device='D0',
                    read_size=510, 
                    data_type=DT.SWORD
                )
            for i in range(dcycle_lenght-3):
                index = dcycle_index[i]
                RealValue = read_result[index].value
                Name = dcycle_name[i]
                Device = read_result[index].device
                if dcycle_old[i] != RealValue:
                    store_and_pubish_data(count, Name, Device, RealValue, 'Cycle')
                    dcycle_old[i] = RealValue
                    count+=1
            with lock:
                read_result1 = plc.batch_read(
                    ref_device='D750',
                    read_size=10, 
                    data_type=DT.SWORD 
                )
            for i in range(dcycle_lenght-2, dcycle_lenght):
                index = dcycle_index[i] # Có thể lỗi chỗ này
                RealValue = read_result1[index].value
                Name = dcycle_name[i]
                Device = read_result1[index].device
                if dcycle_old[i] != RealValue:
                    store_and_pubish_data(count, Name, Device, RealValue, 'Cycle')
                    dcycle_old[i] = RealValue
                    count+=1

        except Exception as e:
            print('task_data_cycle_process')
            print(e)
            reConEth_flg.set()

def task_input_S1():
    t11_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='X0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(dinput_lenght1):
            # if not t11_interupt.is_set():
            #     #print('t11_interupt')
            #     break
            index = dinput_index1[i]
            RealValue = int(read_result[index].value)
            Name = dinput_name1[i]
            Device = dinput_addr1[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Input')
            dinput_old1[i] = RealValue
            count+=1

    except Exception as e:
        print('task_input_S1')
        print(e)
        reConEth_flg.set()
    while True:
        t11_interupt.wait()
        for i in range(dinput_lenght1):
            # if not t11_interupt.is_set():
            #     #print('t11_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='X0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(dinput_lenght1):
                    # if not t11_interupt.is_set():
                    #     #print('t11_interupt')
                    #     break
                    index = dinput_index1[i]
                    RealValue = int(read_result[index].value)
                    Name = dinput_name1[i]
                    Device = dinput_addr1[i]
                    if dinput_old1[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Input')
                        dinput_old1[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_input_S1')
                print(e)
                reConEth_flg.set()

def task_input_S2():
    t13_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='X0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(dinput_lenght2):
            # if not t13_interupt.is_set():
            #     #print('t13_interupt')
            #     break
            index = dinput_index2[i]
            RealValue = int(read_result[index].value)
            Name = dinput_name2[i]
            Device = dinput_addr2[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Input')
            dinput_old2[i] = RealValue
            count+=1

    except Exception as e:
        print('task_input_S2')
        print(e)
        reConEth_flg.set()
    while True:
        t13_interupt.wait()
        for i in range(dinput_lenght2):
            # if not t13_interupt.is_set():
            #     #print('t13_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='X0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(dinput_lenght2):
                    # if not t13_interupt.is_set():
                    #     #print('t13_interupt')
                    #     break
                    index = dinput_index2[i]
                    RealValue = int(read_result[index].value)
                    Name = dinput_name2[i]
                    Device = dinput_addr2[i]
                    if dinput_old2[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Input')
                        dinput_old2[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_input_S2')
                print(e)
                reConEth_flg.set() 

def task_input_S3():
    t15_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='X0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(dinput_lenght3):
            # if not t15_interupt.is_set():
            #     #print('t15_interupt')
            #     break
            index = dinput_index3[i]
            RealValue = int(read_result[index].value)
            Name = dinput_name3[i]
            Device = dinput_addr3[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Input')
            dinput_old3[i] = RealValue
            count+=1

    except Exception as e:
        print('task_input_S3')
        print(e)
        reConEth_flg.set()
    while True:
        t15_interupt.wait()
        for i in range(dinput_lenght3):
            # if not t15_interupt.is_set():
            #     #print('t15_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='X0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(dinput_lenght3):
                    # if not t15_interupt.is_set():
                    #     #print('t15_interupt')
                    #     break
                    index = dinput_index3[i]
                    RealValue = int(read_result[index].value)
                    Name = dinput_name3[i]
                    Device = dinput_addr3[i]
                    if dinput_old3[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Input')
                        dinput_old3[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_input_S3')
                print(e)
                reConEth_flg.set()    

def task_input_S4():
    t17_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='X0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(dinput_lenght4):
            # if not t17_interupt.is_set():
            #     #print('t17_interupt')
            #     break
            index = dinput_index4[i]
            RealValue = int(read_result[index].value)
            Name = dinput_name4[i]
            Device = dinput_addr4[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Input')
            dinput_old4[i] = RealValue
            count+=1

    except Exception as e:
        print('task_input_S4')
        print(e)
        reConEth_flg.set()
    while True:
        t17_interupt.wait()
        for i in range(dinput_lenght4):
            # if not t17_interupt.is_set():
            #     #print('t17_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='X0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(dinput_lenght4):
                    # if not t17_interupt.is_set():
                    #     #print('t17_interupt')
                    #     break
                    index = dinput_index4[i]
                    RealValue = int(read_result[index].value)
                    Name = dinput_name4[i]
                    Device = dinput_addr4[i]
                    if dinput_old4[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Input')
                        dinput_old4[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_input_S4')
                print(e)
                reConEth_flg.set()      

def task_input_S5():
    t19_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='X0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(dinput_lenght5):
            # if not t19_interupt.is_set():
            #     #print('t19_interupt')
            #     break
            index = dinput_index5[i]
            RealValue = int(read_result[index].value)
            Name = dinput_name5[i]
            Device = dinput_addr5[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Input')
            dinput_old5[i] = RealValue
            count+=1

    except Exception as e:
        print('task_input_S5')
        print(e)
        reConEth_flg.set()
    while True:
        t19_interupt.wait()
        for i in range(dinput_lenght5):
            # if not t19_interupt.is_set():
            #     #print('t19_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='X0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(dinput_lenght5):
                    # if not t19_interupt.is_set():
                    #     #print('t19_interupt')
                    #     break
                    index = dinput_index5[i]
                    RealValue = int(read_result[index].value)
                    Name = dinput_name5[i]
                    Device = dinput_addr5[i]
                    if dinput_old5[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Input')
                        dinput_old5[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_input_S5')
                print(e)
                reConEth_flg.set()

def task_input_S6():
    t21_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='X0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(dinput_lenght6):
            # if not t21_interupt.is_set():
            #     #print('t21_interupt')
            #     break
            index = dinput_index6[i]
            RealValue = int(read_result[index].value)
            Name = dinput_name6[i]
            Device = dinput_addr6[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Input')
            dinput_old6[i] = RealValue
            count+=1

    except Exception as e:
        print('task_input_S6')
        print(e)
        reConEth_flg.set()
    while True:
        t21_interupt.wait()
        for i in range(dinput_lenght6):
            # if not t21_interupt.is_set():
            #     #print('t21_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='X0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(dinput_lenght6):
                    # if not t21_interupt.is_set():
                    #     #print('t21_interupt')
                    #     break
                    index = dinput_index6[i]
                    RealValue = int(read_result[index].value)
                    Name = dinput_name6[i]
                    Device = dinput_addr6[i]
                    if dinput_old6[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Input')
                        dinput_old6[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_input_S6')
                print(e)
                reConEth_flg.set()

def task_input_S7():
    t23_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='X0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(dinput_lenght7):
            # if not t23_interupt.is_set():
            #     #print('t23_interupt')
            #     break
            index = dinput_index7[i]
            RealValue = int(read_result[index].value)
            Name = dinput_name7[i]
            Device = dinput_addr7[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Input')
            dinput_old7[i] = RealValue
            count+=1

    except Exception as e:
        print('task_input_S7')
        print(e)
        reConEth_flg.set()
    while True:
        t23_interupt.wait()
        for i in range(dinput_lenght7):
            # if not t23_interupt.is_set():
            #     #print('t23_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='X0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(dinput_lenght7):
                    # if not t23_interupt.is_set():
                    #     #print('t23_interupt')
                    #     break
                    index = dinput_index7[i]
                    RealValue = int(read_result[index].value)
                    Name = dinput_name7[i]
                    Device = dinput_addr7[i]
                    if dinput_old7[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Input')
                        dinput_old7[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_input_S7')
                print(e)
                reConEth_flg.set()
def task_input_S10():
    t25_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='X0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(dinput_lenght10):
            # if not t25_interupt.is_set():
            #     #print('t25_interupt')
            #     break
            index = dinput_index10[i]
            RealValue = int(read_result[index].value)
            Name = dinput_name10[i]
            Device = dinput_addr10[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Input')
            dinput_old10[i] = RealValue
            count+=1

    except Exception as e:
        print('task_input_S10')
        print(e)
        reConEth_flg.set()
    while True:
        t25_interupt.wait()
        for i in range(dinput_lenght10):
            # if not t25_interupt.is_set():
            #     #print('t25_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='X0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(dinput_lenght10):
                    # if not t25_interupt.is_set():
                    #     #print('t25_interupt')
                    #     break
                    index = dinput_index10[i]
                    RealValue = int(read_result[index].value)
                    Name = dinput_name10[i]
                    Device = dinput_addr10[i]
                    if dinput_old10[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Input')
                        dinput_old10[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_input_S10')
                print(e)
                reConEth_flg.set()

def task_input_S11():
    t27_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='X0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(dinput_lenght11):
            # if not t27_interupt.is_set():
            #     #print('t27_interupt')
            #     break
            index = dinput_index11[i]
            RealValue = int(read_result[index].value)
            Name = dinput_name11[i]
            Device = dinput_addr11[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Input')
            dinput_old11[i] = RealValue
            count+=1

    except Exception as e:
        print('task_input_S11')
        print(e)
        reConEth_flg.set()
    while True:
        t27_interupt.wait()
        for i in range(dinput_lenght11):
            # if not t27_interupt.is_set():
            #     #print('t27_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='X0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(dinput_lenght11):
                    # if not t27_interupt.is_set():
                    #     #print('t27_interupt')
                    #     break
                    index = dinput_index11[i]
                    RealValue = int(read_result[index].value)
                    Name = dinput_name11[i]
                    Device = dinput_addr11[i]
                    if dinput_old11[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Input')
                        dinput_old11[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_input_S11')
                print(e)
                reConEth_flg.set()

def task_input_S12():
    t29_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='X0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(dinput_lenght12):
            # if not t29_interupt.is_set():
            #     #print('t29_interupt')
            #     break
            index = dinput_index12[i]
            RealValue = int(read_result[index].value)
            Name = dinput_name12[i]
            Device = dinput_addr12[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Input')
            dinput_old12[i] = RealValue
            count+=1

    except Exception as e:
        print('task_input_S12')
        print(e)
        reConEth_flg.set()
    while True:
        t29_interupt.wait()
        for i in range(dinput_lenght12):
            # if not t29_interupt.is_set():
            #     #print('t29_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='X0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(dinput_lenght12):
                    # if not t29_interupt.is_set():
                    #     #print('t29_interupt')
                    #     break
                    index = dinput_index12[i]
                    RealValue = int(read_result[index].value)
                    Name = dinput_name12[i]
                    Device = dinput_addr12[i]
                    if dinput_old12[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Input')
                        dinput_old12[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_input_S12')
                print(e)
                reConEth_flg.set()

def task_output_S1():
    t12_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='Y0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(doutput_lenght1):
            # if not t12_interupt.is_set():
            #     #print('t12_interupt')
            #     break
            index = doutput_index1[i]
            RealValue = int(read_result[index].value)
            Name = doutput_name1[i]
            Device = doutput_addr1[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Output')
            doutput_old1[i] = RealValue
            count+=1

    except Exception as e:
        print('task_output_S1')
        print(e)
        reConEth_flg.set()
    while True:
        t12_interupt.wait()
        for i in range(doutput_lenght1):
            # if not t12_interupt.is_set():
            #     #print('t12_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='Y0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(doutput_lenght1):
                    # if not t12_interupt.is_set():
                    #     #print('t12_interupt')
                    #     break
                    index = doutput_index1[i]
                    RealValue = int(read_result[index].value)
                    Name = doutput_name1[i]
                    Device = doutput_addr1[i]
                    if doutput_old1[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Output')
                        doutput_old1[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_output_S1')
                print(e)
                reConEth_flg.set()

def task_output_S2():
    t14_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='Y0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(doutput_lenght2):
            # if not t14_interupt.is_set():
            #     #print('t14_interupt')
            #     break
            index = doutput_index2[i]
            RealValue = int(read_result[index].value)
            Name = doutput_name2[i]
            Device = doutput_addr2[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Output')
            doutput_old2[i] = RealValue
            count+=1

    except Exception as e:
        print('task_output_S2')
        print(e)
        reConEth_flg.set()
    while True:
        t14_interupt.wait()
        for i in range(doutput_lenght2):
            # if not t14_interupt.is_set():
            #     #print('t14_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='Y0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(doutput_lenght2):
                    # if not t14_interupt.is_set():
                    #     #print('t14_interupt')
                    #     break
                    index = doutput_index2[i]
                    RealValue = int(read_result[index].value)
                    Name = doutput_name2[i]
                    Device = doutput_addr2[i]
                    if doutput_old2[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Output')
                        doutput_old2[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_output_S2')
                print(e)
                reConEth_flg.set()

def task_output_S3():
    t16_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='Y0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(doutput_lenght3):
            # if not t16_interupt.is_set():
            #     #print('t16_interupt')
            #     break
            index = doutput_index3[i]
            RealValue = int(read_result[index].value)
            Name = doutput_name3[i]
            Device = doutput_addr3[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Output')
            doutput_old3[i] = RealValue
            count+=1

    except Exception as e:
        print('task_output_S3')
        print(e)
        reConEth_flg.set()
    while True:
        t16_interupt.wait()
        for i in range(doutput_lenght3):
            # if not t16_interupt.is_set():
            #     #print('t16_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='Y0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(doutput_lenght3):
                    # if not t16_interupt.is_set():
                    #     #print('t16_interupt')
                    #     break
                    index = doutput_index3[i]
                    RealValue = int(read_result[index].value)
                    Name = doutput_name3[i]
                    Device = doutput_addr3[i]
                    if doutput_old3[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Output')
                        doutput_old3[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_output_S3')
                print(e)
                reConEth_flg.set()

def task_output_S4():
    t18_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='Y0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(doutput_lenght4):
            # if not t18_interupt.is_set():
            #     #print('t18_interupt')
            #     break
            index = doutput_index4[i]
            RealValue = int(read_result[index].value)
            Name = doutput_name4[i]
            Device = doutput_addr4[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Output')
            doutput_old4[i] = RealValue
            count+=1

    except Exception as e:
        print('task_output_S4')
        print(e)
        reConEth_flg.set()
    while True:
        t18_interupt.wait()
        for i in range(doutput_lenght4):
            # if not t18_interupt.is_set():
            #     #print('t18_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='Y0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(doutput_lenght4):
                    # if not t18_interupt.is_set():
                    #     #print('t18_interupt')
                    #     break
                    index = doutput_index4[i]
                    RealValue = int(read_result[index].value)
                    Name = doutput_name4[i]
                    Device = doutput_addr4[i]
                    if doutput_old4[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Output')
                        doutput_old4[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_output_S4')
                print(e)
                reConEth_flg.set()

def task_output_S5():
    t20_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='Y0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(doutput_lenght5):
            # if not t20_interupt.is_set():
            #     #print('t20_interupt')
            #     break
            index = doutput_index5[i]
            RealValue = int(read_result[index].value)
            Name = doutput_name5[i]
            Device = doutput_addr5[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Output')
            doutput_old5[i] = RealValue
            count+=1

    except Exception as e:
        print('task_output_S5')
        print(e)
        reConEth_flg.set()
    while True:
        t20_interupt.wait()
        for i in range(doutput_lenght5):
            # if not t20_interupt.is_set():
            #     #print('t20_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='Y0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(doutput_lenght5):
                    # if not t20_interupt.is_set():
                    #     #print('t20_interupt')
                    #     break
                    index = doutput_index5[i]
                    RealValue = int(read_result[index].value)
                    Name = doutput_name5[i]
                    Device = doutput_addr5[i]
                    if doutput_old5[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Output')
                        doutput_old5[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_output_S5')
                print(e)
                reConEth_flg.set()

def task_output_S6():
    t22_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='Y0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(doutput_lenght6):
            # if not t22_interupt.is_set():
            #     #print('t22_interupt')
            #     break
            index = doutput_index6[i]
            RealValue = int(read_result[index].value)
            Name = doutput_name6[i]
            Device = doutput_addr6[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Output')
            doutput_old6[i] = RealValue
            count+=1

    except Exception as e:
        print('task_output_S6')
        print(e)
        reConEth_flg.set()
    while True:
        t22_interupt.wait()
        for i in range(doutput_lenght6):
            # if not t22_interupt.is_set():
            #     #print('t22_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='Y0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(doutput_lenght6):
                    # if not t22_interupt.is_set():
                    #     #print('t22_interupt')
                    #     break
                    index = doutput_index6[i]
                    RealValue = int(read_result[index].value)
                    Name = doutput_name6[i]
                    Device = doutput_addr6[i]
                    if doutput_old6[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Output')
                        doutput_old6[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_output_S6')
                print(e)
                reConEth_flg.set()

def task_output_S7():
    t24_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='Y0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(doutput_lenght7):
            # if not t24_interupt.is_set():
            #     #print('t24_interupt')
            #     break
            index = doutput_index7[i]
            RealValue = int(read_result[index].value)
            Name = doutput_name7[i]
            Device = doutput_addr7[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Output')
            doutput_old7[i] = RealValue
            count+=1

    except Exception as e:
        print('task_output_S7')
        print(e)
        reConEth_flg.set()
    while True:
        t24_interupt.wait()
        for i in range(doutput_lenght7):
            # if not t24_interupt.is_set():
            #     #print('t24_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='Y0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(doutput_lenght7):
                    # if not t24_interupt.is_set():
                    #     #print('t24_interupt')
                    #     break
                    index = doutput_index7[i]
                    RealValue = int(read_result[index].value)
                    Name = doutput_name7[i]
                    Device = doutput_addr7[i]
                    if doutput_old7[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Output')
                        doutput_old7[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_output_S7')
                print(e)
                reConEth_flg.set()

def task_output_S10():
    t26_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='Y0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(doutput_lenght10):
            # if not t26_interupt.is_set():
            #     #print('t26_interupt')
            #     break
            index = doutput_index10[i]
            RealValue = int(read_result[index].value)
            Name = doutput_name10[i]
            Device = doutput_addr10[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Output')
            doutput_old10[i] = RealValue
            count+=1

    except Exception as e:
        print('task_output_S10')
        print(e)
        reConEth_flg.set()
    while True:
        t26_interupt.wait()
        for i in range(doutput_lenght10):
            # if not t26_interupt.is_set():
            #     #print('t26_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='Y0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(doutput_lenght10):
                    # if not t26_interupt.is_set():
                    #     #print('t26_interupt')
                    #     break
                    index = doutput_index10[i]
                    RealValue = int(read_result[index].value)
                    Name = doutput_name10[i]
                    Device = doutput_addr10[i]
                    if doutput_old10[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Output')
                        doutput_old10[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_output_S10')
                print(e)
                reConEth_flg.set()

def task_output_S11():
    t28_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='Y0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(doutput_lenght11):
            # if not t28_interupt.is_set():
            #     #print('t28_interupt')
            #     break
            index = doutput_index11[i]
            RealValue = int(read_result[index].value)
            Name = doutput_name11[i]
            Device = doutput_addr11[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Output')
            doutput_old11[i] = RealValue
            count+=1

    except Exception as e:
        print('task_output_S11')
        print(e)
        reConEth_flg.set()
    while True:
        t28_interupt.wait()
        for i in range(doutput_lenght11):
            # if not t28_interupt.is_set():
            #     #print('t28_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='Y0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(doutput_lenght11):
                    # if not t28_interupt.is_set():
                    #     #print('t28_interupt')
                    #     break
                    index = doutput_index11[i]
                    RealValue = int(read_result[index].value)
                    Name = doutput_name11[i]
                    Device = doutput_addr11[i]
                    if doutput_old11[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Output')
                        doutput_old11[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_output_S11')
                print(e)
                reConEth_flg.set()

def task_output_S12():
    t30_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='Y0000',
                read_size=250, 
                data_type=DT.BIT
            )
        for i in range(doutput_lenght12):
            # if not t30_interupt.is_set():
            #     #print('t30_interupt')
            #     break
            index = doutput_index12[i]
            RealValue = int(read_result[index].value)
            Name = doutput_name12[i]
            Device = doutput_addr12[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Output')
            doutput_old12[i] = RealValue
            count+=1

    except Exception as e:
        print('task_output_S12')
        print(e)
        reConEth_flg.set()
    while True:
        t30_interupt.wait()
        for i in range(doutput_lenght12):
            # if not t30_interupt.is_set():
            #     #print('t30_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='Y0000',
                        read_size=250, 
                        data_type=DT.BIT
                    )
                for i in range(doutput_lenght12):
                    # if not t30_interupt.is_set():
                    #     #print('t30_interupt')
                    #     break
                    index = doutput_index12[i]
                    RealValue = int(read_result[index].value)
                    Name = doutput_name12[i]
                    Device = doutput_addr12[i]
                    if doutput_old12[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Output')
                        doutput_old12[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_output_S12')
                print(e)
                reConEth_flg.set()
def task_checkpressure_S10():
    t31_interupt.wait()
    count = 0
    try:
        with lock:
            read_result = plc.batch_read(
                ref_device='M2500',
                read_size=80, 
                data_type=DT.BIT
            )
        for i in range(dpressure_lenght):
            # if not t31_interupt.is_set():
            #     #print('t31_interupt')
            #     break
            index = dpressure_index[i]
            RealValue = int(read_result[index].value)
            Name = dpressure_name[i]
            Device = dpressure_addr[i]
            store_and_pubish_data(count, Name, Device, RealValue, 'Output')
            dpressure_old[i] = RealValue
            count+=1

    except Exception as e:
        print('task_checkpressure_S10')
        print(e)
        reConEth_flg.set()
    while True:
        t31_interupt.wait()
        for i in range(dpressure_lenght):
            # if not t31_interupt.is_set():
            #     #print('t31_interupt')
            #     break
            time.sleep(0.005)
            try:
                with lock:
                    read_result = plc.batch_read(
                        ref_device='M2500',
                        read_size=80, 
                        data_type=DT.BIT
                    )
                for i in range(dpressure_lenght):
                    # if not t31_interupt.is_set():
                    #     #print('t31_interupt')
                    #     break
                    index = dpressure_index[i]
                    RealValue = int(read_result[index].value)
                    Name = dpressure_name[i]
                    Device = dpressure_addr[i]
                    if dpressure_old[i] != RealValue:
                        store_and_pubish_data(count, Name, Device, RealValue, 'Output')
                        dpressure_old[i] = RealValue
                        count+=1

            except Exception as e:
                print('task_checkpressure_S12')
                print(e)
                reConEth_flg.set()
def enable_stationandencoder():
    while True:
        time.sleep(1)
        client4.on_message = on_message
        client4.subscribe(topic_subcribe)
def task_machineStatus_process():
    global status_old
    '''
    M1	    MANUAL MODE
    M0	    AUTO ON
    M170	ALL B/F FLT
    M3	    FLT FLG
    Y4	    INDEX MOTOR ON
    T4	    M/C IDLE TD
    SM400    POWER ON
    '''
    count = 0
    status_new = -1

    while True:
        try:
            for i in range(len(list_status_addr)):
                t6_interupt.wait()
                time.sleep(0.01)
                with lock:
                    read_result = plc.batch_read(
                        ref_device=str(list_status_addr[i]),
                        read_size=1, 
                        data_type=DT.BIT 
                    )
                if list_status_old[i] != read_result[0].value:
                    list_status_old[i] = read_result[0].value
                    value = read_result[0]
                    print(f'{count}: ',value.device, value.value)

            M1 = list_status_old[0]
            M0 = list_status_old[1]
            Y4 = list_status_old[2]
            M3 = list_status_old[3]
            T4 = list_status_old[4]
            SM400 = list_status_old[5]

            if SM400:
                if M0 and Y4 and (not M3) and (not T4):
                    status_new = ST.Run
                    if status_old != status_new:
                        store_and_publish_status(count, 'Run', ST.Run)
                        status_old = status_new
                        count += 1

                        # if initRunSt and not is_connectWifi:
                        #     timestamp = datetime.now().isoformat(timespec='microseconds')
                        #     runStTimestamp = generate_data_disconnectWifi('machineStatus', ST.Run, timestamp)
                        #     initRunSt = 0

                elif T4 and (not Y4) and (not M3) and status_old!=ST.Setup and status_old!=ST.On:
                    status_new = ST.Idle
                    if status_old != status_new:
                        store_and_publish_status(count, 'Idle', ST.Idle)
                        status_old = status_new
                        count += 1

                elif (M3):
                    status_new = ST.Alarm
                    if status_old != status_new:
                        store_and_publish_status(count, 'Alarm', ST.Alarm)
                        status_old = status_new
                        count += 1

                elif M1 and Y4 and (not M3) and (not T4):
                    status_new = ST.Setup
                    if status_old != status_new:
                        store_and_publish_status(count, 'Setup', ST.Setup)
                        status_old = status_new
                        count += 1

        except Exception as e:
            print('task_machineStatus_process')
            print(e)
            reConEth_flg.set()


def task_reconnect_ethernetPLC():
    global status_old
    """
    -handles connect/disconnect/reconnect
    -connection-monitoring with cyclic read of the service-level
    """
    __HOST = '192.168.1.250' # REQUIRED
    __PORT = 4095           # OPTIONAL: default is 5007
    while True:
        reConEth_flg.wait()
        print('PLC disconnect!')

        t1_interupt.clear()
        t2_interupt.clear()
        t3_interupt.clear()
        t4_interupt.clear() #Ban đầu này là t10_interupt.clear()
        t5_interupt.clear()
        t6_interupt.clear()
        t7_interupt.clear()
        t8_interupt.clear()
        t9_interupt.clear()
        t10_interupt.clear()
        t11_interupt.clear()
        t12_interupt.clear()
        t13_interupt.clear()
        t14_interupt.clear()
        t15_interupt.clear()
        t16_interupt.clear()
        t17_interupt.clear()
        t18_interupt.clear()
        t19_interupt.clear()
        t20_interupt.clear()
        t21_interupt.clear()
        t22_interupt.clear()
        t23_interupt.clear()
        t24_interupt.clear()
        t25_interupt.clear()
        t26_interupt.clear()
        t27_interupt.clear()
        t28_interupt.clear()
        t29_interupt.clear()
        t30_interupt.clear()
        t31_interupt.clear()


        store_and_publish_status(ST.Off, 'Off', ST.Off)

        case = 0
        count = 0
        alwaysOn = 'SM400'
        
        while True:
            print(case)
            if case == 1:
                # connect
                print("connecting...")
                try:
                    plc.connect(__HOST, __PORT)
                    print("connected!")

                    case = 2
                except Exception as e:
                    print("connection error:", e)
                    case = 1
                    time.sleep(5)
            elif case == 2:
                # running => read cyclic the service level if it fails disconnect and unsubscribe => wait 5s => connect
                try:
                    with lock:
                        read_result = plc.batch_read(
                            ref_device=alwaysOn,
                            read_size=1, 
                            data_type=DT.BIT, 
                        )

                    service_level = read_result[0].value
                    print("service level:", service_level)
                    if service_level:
                        count += 1
                        case = 2
                        time.sleep(0.2)
                        if count == 5:
                            count = 0
                            break
                    else:
                        case = 3
                        time.sleep(5)
                except Exception as e:
                    print("error during operation:", e)
                    case = 3
            elif case == 3:
                # disconnect
                print("disconnecting...")
                try:
                    plc.close()
                except Exception as e:
                    print("disconnection error:", e)
                case = 0
            else:
                # wait
                case = 1
                time.sleep(5)

        store_and_publish_status(status_old, 'status_old', status_old)
        reConEth_flg.clear()

        t1_interupt.set()
        t2_interupt.set()
        t3_interupt.set()
        t4_interupt.set()
        t5_interupt.set()
        t6_interupt.set()
        t7_interupt.set()
        t8_interupt.set()
        t9_interupt.set()
        # t10_interupt.set()
        # t11_interupt.set()
        # t12_interupt.set()


#---------------------------------------------------------------------------

if __name__ == '__main__':
    
    t1 = threading.Thread(target=task_data_setting_process)
    t2 = threading.Thread(target=task_data_count_process)
    t3 = threading.Thread(target=task_data_checking_process)
    t4 = threading.Thread(target=task_data_alarm_process)
    t5 = threading.Thread(target=task_machineStatus_process)
    t6 = threading.Thread(target=task_publish_operationTime)
    t9 = threading.Thread(target=task_data_cycle_process)
    t10 = threading.Thread(target=task_display_tablePosition)
    t11 = threading.Thread(target=task_input_S1)
    t12 = threading.Thread(target=task_output_S1)
    t13 = threading.Thread(target=task_input_S2)
    t14 = threading.Thread(target=task_output_S2)
    t15 = threading.Thread(target=task_input_S3)
    t16 = threading.Thread(target=task_output_S3)
    t17 = threading.Thread(target=task_input_S4)
    t18 = threading.Thread(target=task_output_S4)
    t19 = threading.Thread(target=task_input_S5)
    t20 = threading.Thread(target=task_output_S5)
    t21 = threading.Thread(target=task_input_S6)
    t22 = threading.Thread(target=task_output_S6)
    t23 = threading.Thread(target=task_input_S7)
    t24 = threading.Thread(target=task_output_S7)
    t25 = threading.Thread(target=task_input_S10)
    t26 = threading.Thread(target=task_output_S10)
    t27 = threading.Thread(target=task_input_S11)
    t28 = threading.Thread(target=task_output_S11)
    t29 = threading.Thread(target=task_input_S12)
    t30 = threading.Thread(target=task_output_S12)
    t31 = threading.Thread(target=task_checkpressure_S10)
    task_enable_stationandencoder = threading.Thread(target=enable_stationandencoder)
    task_reconnect_ehtPLC = threading.Thread(target=task_reconnect_ethernetPLC)

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t9.start()
    t10.start()
    t11.start()
    t12.start()
    t13.start()
    t14.start()
    t15.start()
    t16.start()
    t17.start()
    t18.start()
    t19.start()
    t20.start()
    t21.start()
    t22.start()
    t23.start()
    t24.start()
    t25.start()
    t26.start()
    t27.start()
    t28.start()
    t29.start()
    t30.start()
    t31.start()
    task_enable_stationandencoder.start()
    task_reconnect_ehtPLC.start()

    # t1.join()
    # t2.join()
    # t3.join()
    # t4.join()
    # t5.join()
    # t6.join()
    # t9.join()
    # t10.join()
    # t11.join()
    # t12.join()
    # task_reconnect_ehtPLC.join()

        