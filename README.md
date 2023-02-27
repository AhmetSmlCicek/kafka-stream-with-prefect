# kafka-stream-with-prefect

# About
The goal of project is to stream real-time data from UK National railway service. Railway service provides streaming api which is an SOAP API. You can access the API thorugh Python zeep library. 

# Design
Tools prefered for this project: Apache Kafka and Prefect. With help of Kafka, we will get all data streamed through API and we will save it in a topic by our Kafka producer. And then, we can read data with help of our Kafka consumer. 
Prefect2 is chosen to create a workflow for our data pipeline. Prefect offer a very user-friendly UI and it is easy to set up. One of the great feature of Prefect is their so-called blocks. In blocks, you can save your cloud credentials, folder path and even run-time infrastructure such as Docker.
The pipeline is designed as ELT pipeline and consist of different components including cloud solutions.

![Screenshot 2023-02-27 at 09 38 57](https://user-images.githubusercontent.com/113132841/221514818-57aeeed2-0972-4251-95fc-312b9ba223b3.png)


# Run
To run the project, you should have Kafka and Prefect installed in your machine. For this please check, https://kafka.apache.org/quickstart and https://docs.prefect.io/getting-started/overview/. After you have them (and make sure the are running in your machine), all you need to do is to deploy your workflow with command of "prefect deployment run boss/kafka-train". By deploying, you can make use of Prefect trigger so you can schedule your data pipeline as you wish. 

#TO DO
Aim of this project is to create end-to-end data pipeline. After collecting the data, the created files (in csv format) would be stored in Azure Blob containers instead of local machine. From there, we can do simple transformation with Data Factory and display important insights through Power BI.
Cloud integration part will be added later on.
