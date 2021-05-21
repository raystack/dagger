# Create Dagger

This page contains how-to guides for creating Dagger and configure it.

## Prerequisites

Dagger is a streaming processing framework built with Apache Flink to process protobuf data. To run a dagger in any environment you need to have the following things set up beforehand.

#### `Kafka Cluster`

- Dagger use [Kafka](https://kafka.apache.org/) as the source of Data. So you need to set up Kafka(1.0+) either in local or clustered environment. Follow this [quick start](https://kafka.apache.org/quickstart) to set up Kafka in local machine.

#### `Flink [optional]`

- Dagger uses [Apache Flink](https://flink.apache.org/) as the underlying framework for distributed stream processing. The current version of Dagger uses [Flink-1.9](https://ci.apache.org/projects/flink/flink-docs-release-1.9/).

- For distributed stream processing Flink could be configured either on standalone cluster or in one of the supported resource manager. To know more about deploying Flink on production read the deployment section[update link].

- If you want to run dagger in local machine on a single process, you don't actually need to install Flink. The following gradle command should be able to do so.

```
$ ./gradlew runFlink
```

- Tu run the Flink jobs in local machine with java jar and local properties run the following commands.

```sh
# creating a jar
./gradlew :dagger-core:jar
# running the jvm process
java -jar build/dagger-core/libs/<dagger-version>.jar ConfigFile=<filepath>
```

#### `Protobuf Data`

- Dagger extensively works for [protobuf](https://developers.google.com/protocol-buffers) encoded data i.e. Dagger consumes protobuf data from kafka topics, do the processing and produces data in protobuf format to a kafka topic(when sink is kafka).
- So you need to push some proto encoded data to a kafka topic to run a dagger. This you can do from any of easily using any of the Kafka client library. Follow this[tutorial](https://www.conduktor.io/how-to-produce-and-consume-protobuf-records-in-apache-kafka/) to produce proto data to a kafka topic.
- Also you need to define the [java compiled the protobuf schema](https://developers.google.com/protocol-buffers/docs/javatutorial) in the class path or use in-house schema registry tool like [stencil](github.com/odpf/stencil) to let dagger know about the data formats. The stencil is a proprietary library that provides an abstraction layer, for schema handling.Schema Caching, dynamic schema updates are features of the stencil client library.

#### `Sinks`

- The current version of dagger supports InfluxDB and Kafka and as supported sinks to push the data after processing. You need to set up the desired sinks beforehand so that data can be pushed seamlessly.

  ##### `Influx Sink`

  - [InfluxDB](https://github.com/influxdata/influxdb) is an opensource time series database with a rich ecosystem. Enabling Influx sink lets dagger solve real time analytics problems and visualize the results in real time in visualization tools like grafana.
  - To you need to set up Opensource version of InfluxDB to use it as a sink. Follow [this](https://docs.influxdata.com/influxdb/v1.8/introduction/install/) to get started with influx. We currently support InfluxDB-1.0+ in dagger.

  ##### `Kafka Sink` :

  - With Kafka sink dagger pushes the processed data as protobuf to a kafka topic.
  - If you have a kafka cluster set up you are good to produce to kafka sink dagger. Enable auto topic creation in Kafka or create a kafka topic beforehand to push the data.

## Common Configurations

- These configurations are mandatory for dagger creation and sink independent. Here you need to set the kafka source level information as well as SQLs required for daggers. In local execution they would be set inside `local.properties` file.
- The streams is a array of json representing logical source configurations and one to one mapped to a kafka topic. You can have more than one stream and each stream can even point to different Kafka topics in different clusters.
- The `FLINK_JOB_ID` define the name of the flink job and `ROWTIME_ATTRIBUTE_NAME` is the [time attribute](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/concepts/time_attributes/) required for stream processing.
- Read more about the mandatory configurations here.

```properties
STREAMS=[
    {
        "TOPIC_NAMES": "sample-kafka-topic",
        "TABLE_NAME": "test-table",
        "PROTO_CLASS_NAME": "com.tests.TestMessage",
        "EVENT_TIMESTAMP_FIELD_INDEX": "5",
        "KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS": "localhost:9092",
        "KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE": "false",
        "KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET": "latest",
        "KAFKA_CONSUMER_CONFIG_GROUP_ID": "sample kafka consumer group",
        "STREAM_NAME": "test-kafka-stream"
    }
]

ROWTIME_ATTRIBUTE_NAME=rowtime
FLINK_JOB_ID=TestDagger
```

## Write SQL

- Dagger is built keeping SQL first philosophy in mind. Though there are other ways to define data processing in dagger like transformers and processors, SQL always play required in dagger. You can define the SQL query on the streaming table as part of a configuration. This is a sample SELECT star query on the about data source.

```properties
SQL_QUERY= SELECT * from `test-table`
```

- Flink has a really good native support for SQLs. All the Flink supported [SQL statements](https://ci.apache.org/projects/flink/flink-docs-master/docs/dev/table/sql/overview/) should be available out of the box for dagger.
- Also you can define custom User Defined Functions(UDFs) to add your SQL logic. We have pre-supported some SQL some UDFs[update link]. Follow this[update link] to add your own UDF to dagger.
- We have noted some of the sample queries here[update link] for different use cases and things that should be considered while writing a dagger query.

## Log Sink

- Dagger provides a log sink to make it easy to consume the processed messages in standard output. A log sink dagger requires the following config to be set.

```properties
SQL_QUERY= SELECT * from `test-table`
```

- Log sink is mostly used to testing and debugging purpose since it just print the output. This is a sample message produced in log sink after a simple count query like
  `SQL_QUERY= SELECT * from table`

```
Show a sample message
```

## Influx Sink

- Influx sink is useful for data analytics or even for data validation while iterating over the business logic.
- For the InfluxDB sink, you have to specify the InfluxDB level information as well as an influx measurement to push the processed data.
- Showing some of the configurations essential for influx sink Dagger. Find more about them here.

```properties
# === sink config ===
SINK_TYPE=log
# === Influx config ===
INFLUX_URL=http://localhost:8086
INFLUX_USERNAME=root
INFLUX_PASSWORD=
INFLUX_DATABASE=DAGGERS
INFLUX_MEASUREMENT_NAME=concurrent_test
INFLUX_BATCH_SIZE=100
INFLUX_FLUSH_DURATION_IN_MILLISECONDS=1000
INFLUX_RETENTION_POLICY=autogen
```

- If used with InfluxDB sink, `tag_` should be appended to the beginning of those columns which you want as dimensions. Dimensions will help you slice the data in InfluxDB-Grafana. InfluxDB tags are essentially the columns on which data are indexed. Find more on influxDB tags here. DO NOT use tag* for high cardinal data points such as customer_id, merchant_id etc unless you provide a filtering condition; this will create tag explosion & kill the InfluxDB. Ensure there is at least one value field present in the query(not starting with tag*).

- The data generated can be visualized in Grafana dashboards integrated with InfluxDB.

## Kafka Sink

- Kafka sink enables aggregated data to be published to Kafka topic in protobuf and is available for consumption by any type of consumer. After a Dagger job is deployed, the new topic is automatically created in the output Kafka cluster if auto creation is enabled.
- Listing some of the configurations essential for kafka sink Dagger. Find more about them here.

```properties
# === sink config ===
SINK_TYPE=kafka
# === Output Kafka config ===
OUTPUT_PROTO_MESSAGE=com.tests.TestMessage
OUTPUT_KAFKA_BROKER=localhost:9092
OUTPUT_KAFKA_TOPIC=test-kafka-output
```

- Dimensions & metrics from the SELECT section in the query should be mapped to field names in the output proto.
- Find more examples on Kafka sink SQL queries here[update link].

## Advanced Data processing

- Dagger's inbuilt SQL enables user some complex stream processing like aggregations, joins, windowing, union etc. Find more of the sample queries here.
- Dagger supports an internal framework called processors to support some of the more complex Data processing beyond SQL. Some of them are,
  - Custom Java code injection (Transformers) [update link]
  - External source support for data enrichment [update link]
  - Large Windowed Streaming query [update link]

`Note` : _You can configure a lot of other advanced configurations related to stream processing(like watermarks) and related to dagger. Please follow the configuration section._

`Note` : _To add a new feature like support for new source/sinks/post processors/udfs please follow the contribution guidelines._
