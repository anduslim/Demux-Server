#!/usr/bin/python

# This gist shows how to create a secured MQTT connection over TLS 1.2
 
import paho.mqtt.client as paho
import ssl
import datetime
import struct
import array
import math
import requests
import json
import logging

logger = logging.getLogger(__name__)
 
EXT_BROKER_URL = "localhost" #"unisense.i2r.a-star.edu.sg";
EXT_BROKER_PORT = 61616
EXT_BROKER_TIMEOUT = 20
DATAAPI_MEASUREMENTS_POST_URL = "http://dataapi.sensesurf.sns-i2r.org/api/v1/measurements"

TYPE2_MODALITIES = [ 'sequence', 'temperature', 'humidity', 'illuminance', 'pir', 'noise', 'int_temperature']
TYPE2_DATAAPI_MODALITIES = [ 'sequence', 'temperature', 'rel_humidity', 'lux', 'pir', 'ns_decibels', 'int_temperature']
TYPE2_MODALITIES_TYPE = [ 'sequence', 'TemperatureMeasurement', 'HumidityMeasurement', 'IlluminanceMeasurement', 'PirMeasurement', 'NoiseMeasurement', 'TemperatureMeasurement']
TYPE2_BYTELENGTH = [ 2, 4, 4, 4, 2, 4, 4]
TYPE3_MODALITIES = [ 'sequence', 'temperature', 'humidity', 'illuminance', 'irradiance', 'int_temperature', 'voltage', 'current', 'percentage_charge']
TYPE3_DATAAPI_MODALITIES = [ 'sequence', 'temperature', 'rel_humidity', 'lux', 'irradiance', 'int_temperature', 'voltage', 'dc_current', 'percentagecharge']
TYPE3_MODALITIES_TYPE = [ 'sequence', 'TemperatureMeasurement', 'HumidityMeasurement', 'IlluminanceMeasurement', 'IrradianceMeasurement', 'TemperatureMeasurement', 'BatteryStatistic', 'CurrentMeasurement', 'BatteryStatistic']
TYPE3_BYTELENGTH = [ 2, 4, 4, 4, 4, 4, 4, 4, 4]

def post_measurements_data_api(modality_type, nodeid, timestamp, seqno, gwtimestamp, dataapi_modality, value):
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
    result = session.post(DATAAPI_MEASUREMENTS_POST_URL, data=json.dumps(payload), headers=headers)
    if '200' not in str(result.status_code):
        print("Error: %s. [%s] modality-%s" % (result.text, nodeid, modality_type))        
    else:
        print("Success:!")

def parse_type3_modalities(client, nodeid, timestamp, seqno, gwtimestamp, bitmap, payload):
    payload_ptr = 0
    bytelength = 0        
    for position in client.bits(bitmap):
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
            post_measurements_data_api(TYPE3_MODALITIES_TYPE[position-1], nodeid, timestamp, seqno, gwtimestamp, TYPE3_DATAAPI_MODALITIES[position-1], value) 

def parse_type2_modalities(client, nodeid, timestamp, seqno, gwtimestamp, bitmap, payload):
    payload_ptr = 0
    bytelength = 0        
    for position in client.bits(bitmap):
        bytelength = TYPE2_BYTELENGTH[position-1]
        if bytelength == 2:
            value = struct.unpack('H', payload[payload_ptr:payload_ptr+bytelength])
        elif bytelength == 4:
            value = struct.unpack('f', payload[payload_ptr:payload_ptr+bytelength])
        payload_ptr += bytelength
        print("Bitmap Position: %s, Modality: %s of length %d, Value %s" % (str(position-1), TYPE2_MODALITIES[position-1], bytelength, str(value[0])))
        if position > 1:
            post_measurements_data_api(TYPE2_MODALITIES_TYPE[position-1], nodeid, timestamp, seqno, gwtimestamp, TYPE2_DATAAPI_MODALITIES[position-1], value) 

TYPE_MAPPING = {2 : parse_type2_modalities,
                3 : parse_type3_modalities,
}

class MQTTDemuxClient:

    def on_connect(self, client, userdata, flags, rc):
        logger.info('Successfully connected to MQTT broker!')
        client.subscribe('sns/+/+/+', 2)
        logger.info('On_connect: Subscribing to sns/+/+/+')
 
    def on_message(self, client, userdata, msg):
 
        messagebyte = bytearray(msg.payload)
        print( "On message. Received topic %s with qos %s." % (msg.topic, str(msg.qos)) )
        topic = msg.topic.split('/')
        
        if topic[3] == "aggregate":
            print("Pass packet to data module")
            publishTopic = topic[0] + '/' + topic[1] + '/' + topic[2] + '/reading/' + topic[3] 
            self.parse_sensor_pkt(messagebyte, topic[2], publishTopic)
 
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

    def parse_sensor_pkt(self, message, nodeid, topic):
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
        #self.parse_type2_modalities(bitMap, message[15:])
        #seqNum, tempReading, humidReading, lightReading, noiseReading = struct.unpack('=Hffff', message[15:])
        #print("seqNum: %d" % seqNum)
        #print("tempReading %f humidReading %f lightReading %f noiseReading %f" % (tempReading, humidReading, lightReading, noiseReading))

    def on_log(self, mosq, obj, level, string):
        print(string)
 
    def __init__(self):
        self.mqttc = paho.Client("demux_sub_pub", clean_session=True, userdata=None, protocol=paho.MQTTv311)
        self.mqttc.on_message = self.on_message
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_publish = self.on_publish
        self.mqttc.on_subscribe = self.on_subscribe
 
        # Uncomment to enable debug messages
        self.mqttc.on_log = self.on_log
        self.mqttc.connect(EXT_BROKER_URL, EXT_BROKER_PORT, EXT_BROKER_TIMEOUT)
        self.mqttc.loop_forever()

#if __name__ == "__main__":
#    test = MQTTDemuxClient()
