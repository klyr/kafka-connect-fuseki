# Fuseki Kafka Sink Connector

The Fuseki Kafka Sink Connector provides a simple link between a Kafka topic with JSON-LD messages to a [Fuseki](https://jena.apache.org/documentation/fuseki2/) SPARKQL server.


## How to use it

* Copy the jar file in the kafka connectors plugin.path.

* Create the `connect-fuseki.properties` configuration file:

        name=fuseki-test
        connector.class=FusekiSinkConnector
        tasks.max=1
        fuseki-server=http://127.0.0.1:3031
        fuseki-dataset=test-dataset
        topics=source.jsonld
        
        value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
        key.converter=org.apache.kafka.connect.converters.ByteArrayConverter

* Run it

        ./bin/connect-standalone.sh config/connect-standalone.properties config/connect-fuseki.properties

## Build

* Install Gradle

* Compile

        $ gradle fatJar

    The fat jar, with all the dependencies will be located in `build/libs/kafka-connect-fuseki-all-0.1.jar`