#!/usr/bin/python

import paho.mqtt.client as paho
import ssl
import datetime
import struct
import array
import math
import requests
import json
import logging
from django.conf import settings

logger = logging.getLogger(__name__)

OLD_DATAAPI_MEASUREMENTS_POST_URL = "http://dataapi.sensesurf.sns-i2r.org/api/v1/measurements"
OLD_DATAAPI_STATISTICS_POST_URL = "http://dataapi.sensesurf.sns-i2r.org/api/v1/statistics"
DATAAPI_MEASUREMENTS_POST_URL = "http://192.168.15.216/api/v1/measurements"
DATAAPI_STATISTICS_POST_URL = "http://192.168.15.216/api/v1/statistics"

TYPE2_MODALITIES = [ 'sequence', 'temperature', 'humidity', 'illuminance', 'pir', 'int_temperature', 'noise']
TYPE2_DATAAPI_MODALITIES = [ 'sequence', 'temperature', 'rel_humidity', 'lux', 'pir', 'int_temperature', 'ns_decibels']
TYPE2_MODALITIES_TYPE = [ 'sequence', 'TemperatureMeasurement', 'HumidityMeasurement', 'IlluminanceMeasurement', 'PirMeasurement', 'TemperatureMeasurement', 'NoiseMeasurement']
TYPE2_BYTELENGTH = [ 2, 4, 4, 4, 2, 4, 4]
TYPE3_MODALITIES = [ 'sequence', 'temperature', 'humidity', 'illuminance', 'irradiance', 'int_temperature', 'voltage', 'current', 'percentage_charge']
TYPE3_DATAAPI_MODALITIES = [ 'sequence', 'temperature', 'rel_humidity', 'lux', 'irradiance', 'int_temperature', 'voltage', 'dc', 'percentagecharge']
TYPE3_MODALITIES_TYPE = [ 'sequence', 'TemperatureMeasurement', 'HumidityMeasurement', 'IlluminanceMeasurement', 'IrradianceMeasurement', 'TemperatureMeasurement', 'BatteryMeasurement', 'CurrentMeasurement', 'BatteryMeasurement']
TYPE3_BYTELENGTH = [ 2, 4, 4, 4, 4, 4, 4, 4, 4]


class MQTTDemuxClient:

    def activemq_on_connect(self, client, userdata, flags, rc):
        print('Successfully connected to activemq broker!!')

    def activemq_on_publish(self, client, userdata, mid):
        print('On activemq publishing: client %s, userdata %s,  mib %s ' % (str(client), str(userdata), str(mid)))

    def activemq_on_disconnect(self, client, userdata, rc):
        print('On disconnect: client %s, userdata %s,  rc %s ' % (str(client), str(userdata), str(rc)))

    def on_connect(self, client, userdata, flags, rc):
        print('\nSuccessfully connected to MQTT broker!!')
        client.subscribe('sns/+/+/+/#', 2)
        logger.debug('On_connect: Subscribing to sns/+/+/+')
 
    def on_message(self, client, userdata, msg):
 
        messagebyte = bytearray(msg.payload)
        print( "On message. Received topic %s with qos %s." % (msg.topic, str(msg.qos)) )
        topic = msg.topic.split('/')
        
        if topic[3] == 'aggregate' and len(topic) == 4:
            print("Pass packet to data module")
            publishTopic = topic[0] + '/' + topic[1] + '/' + topic[2] + '/reading/' + topic[3] 
            self.parse_sensor_pkt(topic[2], publishTopic, messagebyte)
        elif topic[3] == 'statistics' and len(topic) == 5:
            self.parse_statistics_pkt(topic[2], topic[4], messagebyte.decode('utf-8'))
 
    def on_publish(self, client, userdata, mid):
        logger.debug('On publishing mib: %s ' % (str(mid))) 
    
    def on_subscribe(self, client, userdata, mid, granted_qos):
        logger.debug('On Subscribed, mib: %s with qos: %s' % (str(mid), granted_qos))
 
    def bits(self, n):
        shift_bit = 0

        while n > (1<<shift_bit):
            result = n & (1<<shift_bit)
            if result:
                yield shift_bit
            n ^= (1<<shift_bit)
            shift_bit+=1

    def parse_sensor_pkt(self, nodeid, topic, message):
        print("Parsing data packet");
        timestamp, confSeq, verNum, deployID, bitMap, seqno = struct.unpack('<IHIBIH', message[:17])
        RXTimestamp = datetime.datetime.utcfromtimestamp(timestamp).isoformat()
        print("\nGateway Received Timestamp in seconds: %d Date %s" % (timestamp, str(RXTimestamp)))	
        print("configSeq is %d" % confSeq)
        print("VerNum %d" % verNum)
        print("DeploymentID %d" % deployID)
        print("Bitmap %s" % bin(bitMap))
        print("Sequence %d" % seqno)
        lenData = len(message[15:])
        print("\nRemaining Data Length :%d" %lenData)
        result = TYPE_MAPPING[confSeq](self, int(nodeid), timestamp, seqno, timestamp, bitMap, message[15:])
        print("/******* END PACKET PARSING******/\n\n")


    def parse_statistics_pkt(self, nodeid, type, message):
        try:
            print("Parsing statistics packet with message %s" % message)
            if 'uptime' in type:
                timestamp, uptime = message.split(',')
                print('ts=%s, uptime=%s' % (timestamp, uptime))
                print("[Uptime] message:%s" % message)
                post_measurements_statistics_api('UptimeStatistic', nodeid, timestamp, "", ['uptime'], [uptime])
            elif 'route' in type:
                timestamp, src, seqNum, hopCount, numRecordedRoute, sensorNodeID = message.split(',')
                print('ts=%s, src=%s, hc=%s, numRec=%s, SNID=%s' % (timestamp, src, hopCount, numRecordedRoute, sensorNodeID))
                post_measurements_statistics_api('RoutingStatistic', src, timestamp, seqNum, ['hop_count', 'route'], [hopCount, sensorNodeID])
            elif 'hourlyGateway' in type:
                timestamp, totalPkts, totalBytes, sensorNodeID = message.split(',')
                print('ts=%s, totalPkt=%s, totalBytes=%s, sensorNodeId=%s' % (timestamp, totalPkts, totalBytes, sensorNodeID))
                post_measurements_statistics_api('GatewayStatistic', nodeid, timestamp, "", ['hourly_packet_count', 'hourly_byte_count', 'sensor_list'], [totalPkts, totalBytes, sensorNodeID])
            elif 'hourlySensor' in type:
                timestamp, totalPkts, totalBytes, sensorNodeID, pdr = message.split(',')
                print('ts=%s, totalPkt=%s, totalBytes=%s, sensorNodeId=%s, pdr=%s' % (timestamp, totalPkts, totalBytes, sensorNodeID, pdr))
                post_measurements_statistics_api('SensorStatistic', sensorNodeID, timestamp, "", ['hourly_packet_count', 'hourly_byte_count', 'gateway_guid', 'pdr_list'], [totalPkts, totalBytes, nodeid, pdr])
        except ValueError as e:
            logger.error('There was some crazy error', exc_info=True)

    def __init__(self):
        self.mqttc = paho.Client("sensesurf_demux_sub_pub", clean_session=True, userdata=None, protocol=paho.MQTTv311)
        self.mqttc.on_message = self.on_message
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_publish = self.on_publish
        self.mqttc.on_subscribe = self.on_subscribe
        self.mqttc.connect_async(settings.EXT_BROKER_URL, settings.EXT_BROKER_PORT, settings.EXT_BROKER_TIMEOUT)
        print("Settings URL:%s, Port:%s" % (settings.NCS_ACTIVEMQ_URL, str(settings.NCS_ACTIVEMQ_PORT)))
        self.activemqc = paho.Client("sensesurf_activemq_pub", clean_session=True, userdata=None, protocol=paho.MQTTv31)
        self.activemqc.on_connect = self.activemq_on_connect
        self.activemqc.on_publish = self.activemq_on_publish
        self.activemqc.on_disconnect = self.activemq_on_disconnect
        self.activemqc.connect_async(settings.NCS_ACTIVEMQ_URL, settings.NCS_ACTIVEMQ_PORT, settings.NCS_ACTIVEMQ_TIMEOUT)
        self.activemqc.loop_start()
        self.mqttc.loop_forever()

def post_measurements_data_api(pubclient, modality_type, nodeid, timestamp, seqno, gwtimestamp, dataapi_modality, value):
    session = requests.session()
    headers = {'content-type': 'application/json'}
    payload = {'measurement': {
                   'type': modality_type,
                   'node_guid': nodeid,
                   'recorded_at': timestamp,
                   'sequence_number': seqno,
                   'gateway_received_at': gwtimestamp,
                   dataapi_modality: value[0]
                  }
              }
    try:
        result = session.post(OLD_DATAAPI_MEASUREMENTS_POST_URL, data=json.dumps(payload), headers=headers)
        if '200' not in str(result.status_code):
            print("Error in unisense: %s. [%s] modality-%s" % (result.text, nodeid, modality_type))        
        else:
            print("Success posting in unisense!")

        result = session.post(DATAAPI_MEASUREMENTS_POST_URL, data=json.dumps(payload), headers=headers)
        if '200' not in str(result.status_code):
            print("Error in NCS: %s. [%s] modality-%s" % (result.text, nodeid, modality_type))        
        else:
            print("Success posting in NCS!")
        publishTopic = 'sensesurf/measurements'
        activemq_payload = { 'recorded_at': timestamp, dataapi_modality: value[0], 'deviceid': nodeid, 'type': modality_type }
        pubclient.activemqc.publish(publishTopic, str(activemq_payload))
    except Exception as e:
        logger.error('Measurement data api error: %s' % str(e), exc_info=True)        

def post_measurements_statistics_api(type, nodeid, timestamp, seqno, stats_type, value):
    session = requests.session()
    headers = {'content-type': 'application/json'}
    payload = {'statistic': {
                   'type': type,
                   'node_guid': nodeid,
                   'recorded_at': timestamp,
                   'sequence_number': seqno
                  }
              }
    for index, key in enumerate(stats_type):
        payload['statistic'][key] = value[index].replace(':', ',')

    print(payload)
    try:
        result = session.post(OLD_DATAAPI_STATISTICS_POST_URL, data=json.dumps(payload), headers=headers)
        if '200' not in str(result.status_code):
            print("Error in unisense: %s. [%s] type-%s" % (result.text, nodeid, type))        
        else:
            print("Success posting in unisense!")

        result = session.post(DATAAPI_STATISTICS_POST_URL, data=json.dumps(payload), headers=headers)
    except Exception as e:    
        logger.error('Measurements statistics api error: %s' % str(e), exc_info=True)        
        if '200' not in str(result.status_code):
            print("Error: %s. [%s] type-%s" % (result.text, nodeid, type))        
        else:
            print("Success posting in NCS!")


def parse_type3_modalities(client, nodeid, timestamp, seqno, gwtimestamp, bitmap, payload):
    payload_ptr = 0
    bytelength = 0        
    try:
        for position in client.bits(bitmap):
            if position != 0:
                bytelength = TYPE3_BYTELENGTH[position-1]
                if bytelength == 2:
                    value = struct.unpack('H', payload[payload_ptr:payload_ptr+bytelength])
                    print("value is %s" % str(value[0]))
                elif bytelength == 4:
                    value = struct.unpack('f', payload[payload_ptr:payload_ptr+bytelength])
                    print("value is %f" % value)
                payload_ptr += bytelength
                print("Bitmap Position: %s, Modality: %s of length %d, Value %s" % (str(position-1), TYPE3_MODALITIES[position-1], bytelength, str(value[0])))
                if position > 1:
                    post_measurements_data_api(client, TYPE3_MODALITIES_TYPE[position-1], nodeid, timestamp, seqno, gwtimestamp, TYPE3_DATAAPI_MODALITIES[position-1], value) 
    except Exception as e:
        logger.error('Parse type 3 error: %s' % str(e), exc_info=True)        

def parse_type2_modalities(client, nodeid, timestamp, seqno, gwtimestamp, bitmap, payload):
    payload_ptr = 0
    bytelength = 0        
    try:
        for position in client.bits(bitmap):
            if position != 0:
                bytelength = TYPE2_BYTELENGTH[position-1]
                if bytelength == 2:
                    value = struct.unpack('H', payload[payload_ptr:payload_ptr+bytelength])
                elif bytelength == 4:
                    value = struct.unpack('f', payload[payload_ptr:payload_ptr+bytelength])
                payload_ptr += bytelength
                print("Bitmap Position: %s, Modality: %s of length %d, Value %s" % (str(position-1), TYPE2_MODALITIES[position-1], bytelength, str(value[0])))
                if position > 1:
                    post_measurements_data_api(client, TYPE2_MODALITIES_TYPE[position-1], nodeid, timestamp, seqno, gwtimestamp, TYPE2_DATAAPI_MODALITIES[position-1], value) 
    except Exception as e:
        logger.error('Parse type 2 error: %s' % str(e), exc_info=True)        

TYPE_MAPPING = {2 : parse_type2_modalities,
                3 : parse_type3_modalities,
}


