Server configuration
Server1: kafka producer and cluster
Server2: spark application
Server3: moongen packet replay

To start Kafka producer
Start zookeeper:
bin/zookeeper-server-start.sh config/zookeeper.properties
Start kafka cluster:
bin/kafka-server-start.sh config/server.properties
Create topic named 'test':
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test

To run the kakfa producer (sniffer)
In server 1: 
cd into producer_kafka
sudo python3 sniffer.py

To run the spark application
cd into the spark directory from server 2
./spark-2.4.5-bin-hadoop2.7/bin/spark-submit --class "Main" netai_project.jar 
