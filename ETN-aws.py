# Python script to request data from PSE Api, and post it to AWS IoT Core through MQTT. Based on mqtt5_pubsub.py from aws-iot-device-sdk.

from awsiot import mqtt5_client_builder
from awscrt import mqtt5
import threading
from concurrent.futures import Future
import json
import requests
from datetime import date, timedelta, datetime
import configparser
from time import sleep

TIMEOUT = 100
received_all_event = threading.Event()
future_stopped = Future()
future_connection_success = Future()
do_once=True

# LOAD CONFIG FILE #
config = configparser.ConfigParser()
config.read('config.ini')

pse_api_URL = config.get('URL',"pse_api_URL")
iot_endpoint = config.get('URL',"iot_endpoint")
def_topic = config.get('TOPIC', 'def_topic')
Certificate_filePath = config.get('PATH','Certificate_filePath')
Key_filePath = config.get('PATH','Key_filePath')

GetState = config.getboolean('DEFAULT','GetState') 
StateStartRange = config.get('DEFAULT','StateStartRange')
StateEndRange = config.get('DEFAULT','StateEndRange')

GetPrice = config.getboolean('DEFAULT','GetPrice') 
PriceStartRange = config.get('DEFAULT','PriceStartRange')
PriceEndRange = config.get('DEFAULT','PriceEndRange')
# END CONFIG #


# Get date
def today(frmt='%Y-%m-%d', string=True):
    today = date.today() - timedelta(0)
    if string:
        return today.strftime(frmt)
    return today

# Get hour
def hour(frmt='%H', string=True):
    hour = datetime.now()
    if string:
        return hour.strftime(frmt)
    return hour

# Get minute
def minute(frmt='%M', string=True):
    minute = datetime.now()
    if string:
        return minute.strftime(frmt)
    return minute

# Get second
def second(frmt='%S', string=True):
    second = datetime.now()
    if string:
        return second.strftime(frmt)
    return second

def setup_json():
    with open('prices.json','w') as f:
        json.dump({"state": {"desired": {}}},f)
    f.close
            
    with open('states.json','w') as f:
        json.dump({"state": {"desired": {}}},f)
    f.close

# Callback when any publish is received
def on_publish_received(publish_packet_data):
    publish_packet = publish_packet_data.publish_packet
    assert isinstance(publish_packet, mqtt5.PublishPacket)
    print("Received message from topic'{}':{}".format(publish_packet.topic, publish_packet.payload))

# Callback for the lifecycle event Stopped
def on_lifecycle_stopped(lifecycle_stopped_data: mqtt5.LifecycleStoppedData):
    print("Lifecycle Stopped")
    global future_stopped
    future_stopped.set_result(lifecycle_stopped_data)

# Callback for the lifecycle event Connection Success
def on_lifecycle_connection_success(lifecycle_connect_success_data: mqtt5.LifecycleConnectSuccessData):
    print("Lifecycle Connection Success")
    global future_connection_success
    future_connection_success.set_result(lifecycle_connect_success_data)

# Callback for the lifecycle event Connection Failure
def on_lifecycle_connection_failure(lifecycle_connection_failure: mqtt5.LifecycleConnectFailureData):
    print("Lifecycle Connection Failure")
    print("Connection failed with exception:{}".format(lifecycle_connection_failure.exception))

# Publish message
def client_pub(message, topic):
    client.publish(mqtt5.PublishPacket(
        topic=topic,
        payload=json.dumps(message),
        qos=mqtt5.QoS.AT_LEAST_ONCE
        ))

# Main function
def main_prog(client):
    URL = pse_api_URL + "?$filter=business_date eq '"+today()+"'"
    r = requests.get(url = URL,verify=False)    
    data = r.json()
    with open('data.json', 'w') as f:
        json.dump(data, f)
    f.close

    with open('data.json', 'r') as f:
        my_dict = json.load(f)
    f.close
        
    counter = 0
    price = []
    state = []
    for i in range(100):
        price.append(i)
        state.append(i)
    
    for i in my_dict["value"]:
        rce_pln = int(i["rce_pln"])
        price[counter]=rce_pln
        if price[counter]<=1:
            state[counter] = 1
        else:
            state[counter] = 0
        counter=counter+1
    counter = 0
  
    with open('prices.json', 'r') as f:
        my_prices = json.load(f)
    f.close
    
    with open('states.json', 'r') as f:
        my_states = json.load(f)
    f.close

    counter=0
    for x in range(int(StateStartRange),int(StateEndRange)):
        counter+=1
        my_states['state']['desired']["M"+str(x)]=state[counter]
    
    counter=0
    for x in range(int(PriceStartRange),int(PriceEndRange)):
        counter+=1
        my_prices['state']['desired']["MW"+str(x)]=price[counter]

    with open('prices.json','w') as f:
        json.dump(my_prices,f)
    f.close
    
    with open('states.json','w') as f:
        json.dump(my_states,f)
    f.close
    
    if GetState and GetPrice:
        client_pub(my_states,message_topic)
        client_pub(my_prices,message_topic)
    elif GetState:
        client_pub(my_states,message_topic)
    elif GetPrice:
        client_pub(my_prices,message_topic)


    
if __name__ == '__main__':
    message_topic = def_topic
    message_string = 'Hello World!'
    
    # Create MQTT5 client
    client = mqtt5_client_builder.mtls_from_path(
        endpoint=iot_endpoint, 
        port=8883,
        cert_filepath=Certificate_filePath, 
        pri_key_filepath=Key_filePath,
        on_publish_received=on_publish_received,
        on_lifecycle_stopped=on_lifecycle_stopped,
        on_lifecycle_connection_success=on_lifecycle_connection_success,
        on_lifecycle_connection_failure=on_lifecycle_connection_failure)

    client.start()
    lifecycle_connect_success_data = future_connection_success.result(TIMEOUT)
    connack_packet = lifecycle_connect_success_data.connack_packet
    negotiated_settings = lifecycle_connect_success_data.negotiated_settings

    while(True):
        if do_once:
            setup_json()
            main_prog(client)
            do_once=False
            
        if (minute()==0 or int(minute())%15==0) and int(second())>0:
            main_prog(client)
            sleep(60)