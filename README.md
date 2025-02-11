# pinax-redis-kafka-connect
This is a simple Kafka Connect plugin that reads messages from a Kafka topic and writes them to a Redis. This sink also sends email notifications to users that are approaching their usaqge limit.

## Package
To build the package, run the following command:
```bash
mvn clean package
```