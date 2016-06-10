#!/usr/bin/env python

# MqttIpdPublisher v. 1.0
# Publisher of IPD-PowerWorx data to a Backend Device Management via MQTT
# Copyright 2016 Greenlab Engineering Srl


import paho.mqtt.client as mqtt
import configparser
import io
import requests, json
import sys
import linecache
import datetime, pytz
import time
import logging
import logging.handlers
import calendar

# The callback for when the mqtt client receives a CONNACK response from the server.
def onMqttConnect(client, userdata, flags, rc):
    logging.info("Connected with result code "+str(rc))

# The callback for when the mqtt client publishes a data to the server.
def onMqttPublish(client, userdata, mid):
    pass

# Publishing of ipd primary data to IDAS MQTT Agent
def publishDataPrim(data, client, idas):
    dt = datetime.datetime.now(pytz.timezone(IPD_TZ))     
    client.publish(idas+"/ts", dt.isoformat("T"), qos=2, retain=True)
    client.publish(idas+"/v1", data['V1'], qos=2, retain=True)
    client.publish(idas+"/v2", data['V2'], qos=2, retain=True)
    client.publish(idas+"/v3", data['V3'], qos=2, retain=True)
    client.publish(idas+"/i1", data['I1'], qos=2, retain=True)
    client.publish(idas+"/i2", data['I2'], qos=2, retain=True)
    client.publish(idas+"/i3", data['I3'], qos=2, retain=True)
    client.publish(idas+"/iout1", data['IOUT1'], qos=2, retain=True)
    client.publish(idas+"/iout2", data['IOUT2'], qos=2, retain=True)
    client.publish(idas+"/iout3", data['IOUT3'], qos=2, retain=True)
    client.publish(idas+"/s1", data['S1'], qos=2, retain=True)
    client.publish(idas+"/s2", data['S2'], qos=2, retain=True)
    client.publish(idas+"/s3", data['S3'], qos=2, retain=True)
    client.publish(idas+"/p1", data['P1'], qos=2, retain=True)
    client.publish(idas+"/p2", data['P2'], qos=2, retain=True)
    client.publish(idas+"/p3", data['P3'], qos=2, retain=True)
    client.publish(idas+"/q1", data['Q1'], qos=2, retain=True)
    client.publish(idas+"/q2", data['Q2'], qos=2, retain=True)
    client.publish(idas+"/q3", data['Q3'], qos=2, retain=True)
    client.publish(idas+"/qout1", data['QOUT1'], qos=2, retain=True)
    client.publish(idas+"/qout2", data['QOUT2'], qos=2, retain=True)
    client.publish(idas+"/qout3", data['QOUT3'], qos=2, retain=True)
    client.publish(idas+"/freq", data['FREQ1'], qos=2, retain=True)
    client.publish(idas+"/pf1", data['PF1'], qos=2, retain=True)
    client.publish(idas+"/pf2", data['PF2'], qos=2, retain=True)
    client.publish(idas+"/pf3", data['PF3'], qos=2, retain=True)
    client.publish(idas+"/rpf1", data['RPF1'], qos=2, retain=True)
    client.publish(idas+"/rpf2", data['RPF2'], qos=2, retain=True)
    client.publish(idas+"/rpf3", data['RPF3'], qos=2, retain=True)
    client.publish(idas+"/t1", data['T1'], qos=2, retain=True)
    client.publish(idas+"/t2", data['T2'], qos=2, retain=True)
    client.publish(idas+"/t3", data['T3'], qos=2, retain=True)
    client.publish(idas+"/vdcp", data['VDCP'], qos=2, retain=True)
    client.publish(idas+"/vdcm", data['VDCM'], qos=2, retain=True)
    client.publish(idas+"/pds", data['PDS'], qos=2, retain=True)

# Publishing of ipd installation data to IDAS MQTT Agent
def publishDataInst(data, client, idas):
    dt = datetime.datetime.now(pytz.timezone(IPD_TZ))     
    client.publish(idas+"/inst/ts", dt.isoformat("T"), qos=2, retain=True)
    client.publish(idas+"/inst/insttype", data['InstType'], qos=2, retain=True)
    client.publish(idas+"/inst/posgps", data['PosGps'], qos=2, retain=True)
    client.publish(idas+"/inst/ktaprinc", data['kTAPrinc'], qos=2, retain=True)
    client.publish(idas+"/inst/ktaderiv", data['kTADeriv'], qos=2, retain=True)
    client.publish(idas+"/inst/instdate", data['InstDate'], qos=2, retain=True)


#Setup logging
LOG_FILENAME = 'MqttIpdPublish.log'
logging.basicConfig(format='%(asctime)s - %(levelname)s:%(message)s', level=logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s:%(message)s')
filehandler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=100000, backupCount=3)
filehandler.setFormatter(formatter)
logging.getLogger('').addHandler(filehandler)

# Load the configuration file
CONFIG_FILE = "ipd.ini"
config = configparser.RawConfigParser(allow_no_value=True)
config.read(CONFIG_FILE)

IDAS_HOST=config.get('idas', 'host')
IDAS_MQTT_PORT=config.get('idas', 'mqttport')
IDAS_APIKEY=config.get('idas', 'apikey')
FIWARE_SERVICE=config.get('idas', 'fiware-service')
FIWARE_SERVICE_PATH=config.get('idas', 'fiware-service-path')
IDAS_AAA=config.get('idas', 'OAuth')
if IDAS_AAA == "yes":
   TOKEN=config.get('user', 'token')
   TOKEN_SHOW=TOKEN[1:5]+"**********************************************************************"+TOKEN[-5:]
else:
   TOKEN="NULL"
   TOKEN_SHOW="NULL"

IPD_HOST=config.get('ipd', 'host')
IPD_PORT=config.get('ipd', 'port')
IPD_DEVICE_ID=config.get('ipd', 'deviceId')
IPD_FAST_REFRESH=config.get('ipd', 'fastRefreshSeconds')
IPD_SLOW_REFRESH=config.get('ipd', 'slowRefreshMinutes')
IPD_TZ=config.get('ipd', 'timezone')

urlMPrim = "http://"+IPD_HOST+":"+IPD_PORT+'/mprim'
urlIInst = "http://"+IPD_HOST+":"+IPD_PORT+'/iist'

dt = datetime.datetime.now(pytz.timezone(IPD_TZ)) 
print ('* Started at: '+dt.isoformat())
logging.info('Program started.')
first = True
fastSlot = 0
slowSlot = 0

# Main thread
try:
    client = mqtt.Client()
    client.username_pw_set(IDAS_APIKEY)
    client.on_connect = onMqttConnect
    client.on_publish = onMqttPublish
    client.connect(IDAS_HOST, int(IDAS_MQTT_PORT), 60)
    client.loop_start()
    
    while True:
        seconds = calendar.timegm(time.gmtime())
        minutes = int(seconds/60)
        
        # Fast refresh tasks
        if (int(seconds / int(IPD_FAST_REFRESH)) != fastSlot):  
          fastSlot = int(seconds / int(IPD_FAST_REFRESH))
          r=requests.get(urlMPrim, timeout=5)
          if (r.status_code==200):
            data=r.json()
            publishDataPrim(data, client, IDAS_APIKEY+"/"+IPD_DEVICE_ID)
          
          else:
            logging.warning('IPD primary measures request status code: '+str(r.status_code))
          
        # Slow refresh tasks
        if (int(minutes / int(IPD_SLOW_REFRESH)) != slowSlot):  
          slowSlot = int(minutes / int(IPD_SLOW_REFRESH))
          r=requests.get(urlIInst, timeout=5)
          if (r.status_code==200):
            data=r.json()
            publishDataInst(data, client, IDAS_APIKEY+"/"+IPD_DEVICE_ID)
          
          else:
            logging.warning('IPD installation data request status code: '+str(r.status_code))

          
        first = False      
        time.sleep(1)
        
except KeyboardInterrupt:
    logging.warning('Program execution interrupted.')
    client.disconnect()
    client.loop_stop()
    sys.exit(1)
except requests.exceptions.Timeout:
    logging.error('Ipd host not responding.')
    client.disconnect()
    client.loop_stop()
    sys.exit(3)
except Exception:
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    logging.error ('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))
    sys.exit(3)


