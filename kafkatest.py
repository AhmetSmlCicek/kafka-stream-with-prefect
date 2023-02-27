# from websocket import create_connection
# import json
import json
from kafka import KafkaConsumer,TopicPartition
import prefect
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner,ConcurrentTaskRunner
from prefect_dask.task_runners import DaskTaskRunner
from dotenv import load_dotenv
import os
from datetime import date,datetime
from time import sleep
from kafka import KafkaProducer
from zeep import Client, Settings, xsd
from zeep.plugins import HistoryPlugin
import json
import csv
from prefect.filesystems import LocalFileSystem
from prefect.blocks.system import String



@task
def data_stream():
    # Retrive token value from Prefect string block (which can be created on Prefect UI)
    # Preferably is better to use "secret" block at Prefect to store sensitive data such as token
    token = String.load("token")
    

    print(token.value)

    WSDL = 'http://lite.realtime.nationalrail.co.uk/OpenLDBWS/wsdl.aspx?ver=2021-11-01'

    if token == '':
        raise Exception("Please configure your OpenLDBWS token in getDepartureBoardExample!")

    print("Start")

    producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda m: json.dumps(m).encode('ascii'))

    print(producer.bootstrap_connected())

    print("Producer started")

    settings = Settings(strict=False)

    print("Settings:",settings)

    history = HistoryPlugin()

    if Client(wsdl=WSDL, settings=settings, plugins=[history]):
        client = Client(wsdl=WSDL, settings=settings, plugins=[history])
        print("Client created")

    else:
        print("Client not created")

    print("Client connected")

    header = xsd.Element(
        '{http://thalesgroup.com/RTTI/2013-11-28/Token/types}AccessToken',
        xsd.ComplexType([
            xsd.Element(
                '{http://thalesgroup.com/RTTI/2013-11-28/Token/types}TokenValue',
                xsd.String()),
        ])
    )

    header_value = header(TokenValue=token.value)

    res = client.service.GetArrivalBoard(numRows=20, crs='EUS', _soapheaders=[header_value])

    print("Trains at " + res.locationName)
    print("===============================================================================")

    services = res.trainServices.service

    counter = 0

    while True:
        for i in range(len(services)):
            t = services[i]
            if t.eta == "On time":
                info = {"Train_Id":t.serviceID,"Est_arrival_time":t.sta,"Origin":t.origin.location[0].locationName, "Destination":t.destination.location[0].locationName, "Delay_minutes": 0,"Delay_reason":t.delayReason,"Cancel_reason":t.cancelReason}
                print(info)
                producer.send("test_6",info)
                producer.flush()

                print(f"Info sent to consumer:"+str(counter))
                print("===============================================")
            
            
            elif t.eta == "Cancelled":
                info = {"Train_Id":t.serviceID,"Est_arrival_time":t.sta,"Origin":t.origin.location[0].locationName, "Destination":t.destination.location[0].locationName, "Delay_minutes": "Cancelled","Delay_reason":t.delayReason,"Cancel_reason":t.cancelReason}
                print(info)
                producer.send("test_6",info)
                producer.flush()
                
                print(f"Info sent to consumer:"+str(counter))
                print("===============================================")

            elif t.eta == "Delayed":
                info = {"Train_Id":t.serviceID,"Est_arrival_time ":t.sta,"Origin":t.origin.location[0].locationName, "Destination":t.destination.location[0].locationName, "Delay_minutes": "Unknown","Delay_reason":t.delayReason}
                producer.send("test_6",info)
                producer.flush()
                
                print(f"Info sent to consumer:"+str(counter))
                print("===============================================")
                
            else:
                info = {"Train_Id":t.serviceID,"Est_arrival_time":t.sta, "Origin":t.origin.location[0].locationName, "Destination":t.destination.location[0].locationName ,"Delay_minutes": int((datetime.strptime(t.eta, "%H:%M") - datetime.strptime(t.sta, "%H:%M")).total_seconds() / 60),"Delay_reason":t.delayReason,"Cancel_reason":t.cancelReason}
                print(info)
                producer.send("test_6",info)
                producer.flush()
                
                print(f"Info sent to consumer:"+str(counter))
                print("===============================================================================")
            
            counter +=1
        sleep(180)




@task
def get_data():
    
    today = date.today()

    # Getting the data as JSON
    consumer = KafkaConsumer(
        bootstrap_servers=['127.0.0.1:9092'],
        value_deserializer=lambda m: json.loads(m.decode('ascii')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='my-group4',
        #consumer_timeout_ms=5000,
        )

    print("Consumer ready")

    topic = 'test_6'

    # prepare consumer
    tp = TopicPartition(topic,0)
    consumer.assign([tp])
    # lastOffset = consumer.end_offsets([tp])[tp]
    # print("lastOffset:",lastOffset)

    # Local file path retrived from Prefect Local File System (which can be created on Prefect UI)
    local_file = LocalFileSystem.load("data").basepath

    csv_columns = ["Train_Id","Est_arrival_time" ,"Origin" ,"Destination","Delay_minutes","Delay_reason","Cancel_reason"]
    with open(f'{local_file}/{today}.csv', 'a',newline='') as f:
        w = csv.DictWriter(f, fieldnames=csv_columns,extrasaction='ignore')
        w.writeheader()
        for message in consumer:
            print ("Offset:", message.offset)
            lastOffset = consumer.end_offsets([tp])[tp]
            print("lastOffset:",lastOffset)
            print("Message received")
            info = (message.value)
            print(info)
            w.writerow(info)
            print("Message saved")
            # if message.offset == lastOffset-1 :
            #     print("Lastoffset:",consumer.end_offsets([tp])[tp])
            #     print("Done")
            #     break
    #consumer.close()


@flow()
def boss():
    data_stream.submit()
    get_data.submit()
    

if __name__=="__main__":
    boss()


