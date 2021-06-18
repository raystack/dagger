# Configurations

This page contains reference for all the application configurations for Dagger.

## Table of Contents

* [Generic](configuration.md#generic)
* [Influx Sink](configuration.md#influx-sink)
* [Kafka Sink](configuration.md#kafka-sink)
* [Schema Registry](configuration.md#schema-registry)
* [Flink](configuration.md#flink)
* [Dart](configuration.md#dart)
* [Longbow](configuration.md#longbow)
* [PreProcessor](configuration.md#preprocessor)
* [PostProcessor](configuration.md#postprocessor)
* [Telemetry](configuration.md#telemetry)

### Generic

A log sink of Dagger requires the following variables to be set

#### `STREAMS`

Dagger can run on multiple streams, so streams config can con consist of multiple stream. For each stream, there are variables need to be configured.

##### `SOURCE_KAFKA_TOPIC_NAMES`

Defines the list of kafka topics to consume from.

* Example value: `test-topic`
* Type: `required`

##### `INPUT_SCHEMA_TABLE`

Defines the table name for the stream.

* Example value: `data_stream`
* Type: `required`

##### `INPUT_SCHEMA_PROTO_CLASS`

Defines the schema of input proto class of the data from kafka.

* Example value: `com.tests.TestMessage`
* Type: `required`

##### `INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX`

Defines the field index of event timestamp from the input proto class that will be used.

* Example value: `41`
* Type: `required`

##### `SOURCE_KAFKA_CONFIG_BOOTSTRAP_SERVERS`

Defines the bootstrap server of kafka brokers to consume from.

* Example value: `localhost:9092`
* Type: `required`

##### `SOURCE_KAFKA_CONFIG_AUTO_COMMIT_ENABLE`

Enable/Disable kafka consumer auto commit

* Example value: `false`
* Type: `optional`
* Default value: `false`

##### `SOURCE_KAFKA_CONFIG_AUTO_OFFSET_RESET`

Defines kafka consumer offset reset policy

* Example value: `latest`
* Type: `optional`
* Default value: `latest`

##### `SOURCE_KAFKA_CONFIG_GROUP_ID`

Deifnes the kafka consumer group ID for Dagger deployment.

* Example value: `dummy-consumer-group`
* Type: `optional`

##### `SOURCE_KAFKA_NAME`

Defines the stream name for the kafka consumer.

* Example value: `local-kafka-stream`
* Type: `required`

##### Sample Configuration
```
STREAMS = [
   {
      "SOURCE_KAFKA_TOPIC_NAMES": "test-topic",
      "INPUT_SCHEMA_TABLE": "data_stream",
      "INPUT_SCHEMA_PROTO_CLASS": "com.tests.TestMessage",
      "INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX": "41",
      "SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS": "localhost:9092",
      "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE": "false",
      "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET": "latest",
      "SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID": "dummy-consumer-group",
      "SOURCE_KAFKA_NAME": "local-kafka-stream"
   }
]
```

#### `SINK_TYPE`

Defines the Dagger sink type.

* Example value: `log`
* Type: `required`

#### `FLINK_JOB_ID`

Defines the dagger deployment name

* Example value: `SQL Flink job`
* Type: `optional`
* Default value: `SQL Flink job`

#### `FLINK_ROWTIME_ATTRIBUTE_NAME`

Defines the time attribute field name on the data stream

* Example value: `rowtime`
* Type: `required`

#### `FUNCTION_FACTORY_CLASSES`

Defines the factory class of the udf. Multiple factory class could be given in a comma separated format.

* Example value: `io.odpf.dagger.functions.udfs.factories.FunctionFactory`
* Type: `Optional`
* Default value: `io.odpf.dagger.functions.udfs.factories.FunctionFactory`

#### `SQL_QUERY`

Defines the sql query to get the data from data stream.

* Example value: `SELECT * from data_stream`
* Type: `required`

#### `SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE`

Enable/Disable to consume large message from kafka. by default its configuration using default `max.partition.fetch.bytes` kafka config. If set to enable, will set the `max.partition.fetch.bytes`=`5242880`.

* Example value: `false`
* Type: `optional`
* Default value: `false`

### Influx Sink

An Influx sink Dagger \(`SINK_TYPE`=`influx`\) requires the following variables to be set along with Generic ones.

#### `SINK_INFLUX_URL`

InfluxDB URL, it's usually the hostname followed by port.

* Example value: `http://localhost:8086`
* Type: `required`

#### `SINK_INFLUX_USERNAME`

Defines the username to connect to InfluxDB.

* Example value: `root`
* Type: `required`

#### `SINK_INFLUX_PASSWORD`

Defines the password to connect to InfluxDB.

* Example value: `root`
* Type: `required`

#### `SINK_INFLUX_DB_NAME`

Defines the InfluxDB database name.

* Example value: `DAGGER`
* Type: `required`

#### `SINK_INFLUX_MEASUREMENT_NAME`

Defines the InfluxDB measurement name.

* Example value: `concurrent_test`
* Type: `required`

#### `SINK_INFLUX_RETENTION_POLICY`

Defines the InfluxDB retention policy.

* Example value: `autogen`
* Type: `optional`
* Default value: `autogen`

#### `SINK_INFLUX_BATCH_SIZE`

Defines the InfluxDB batch size.

* Example value: `100`
* Type: `optional`
* Default value: `0`

#### `SINK_INFLUX_FLUSH_DURATION_MS`

Defines the InfluxDB flush duration in millis.

* Example value: `1000`
* Type: `optional`
* Default value:  `0`

### Kafka Sink

A Kafka sink Dagger \(`SINK_TYPE`=`kafka`\) requires the following variables to be set along with Generic ones.

#### `SINK_KAFKA_BROKERS`

Defines the list of kafka brokers sink 

* Example value: `localhost:9092`
* Type: `required`

#### `SINK_KAFKA_TOPIC`

Defines the topic of kafka sink

* Example value: `test-kafka-output`
* Type: `required`

#### `SINK_KAFKA_PROTO_KEY`

Defines the proto class key of the data to kafka sink.

* Example value: `com.tests.OutputKey`
* Type: `required`

#### `SINK_KAFKA_PROTO_MESSAGE`

Defines the proto class message of the data to kafka sink.

* Example value: `com.tests.OutputMessage`
* Type: `required`

#### `SINK_KAFKA_STREAM`

Defines the output kafka stream name.

* Example value: `output-stream-name`
* Type: `required`

#### `SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE`

Enable/Disable to produce large message to kafka. by default its configuration using default `max.request.size` kafka config. If set to enable, will set the `max.request.size`=`20971520` and `compression.type`=`snappy`.
 
* Example value: `false`
* Type: `optional`
* Default value: `false`

### Schema Registry

#### `SCHEMA_REGISTRY_STENCIL_ENABLE`

Enable/Disable using Stencil schema registry.

* Example value: `false`
* Type: `optional`
* Default value: `false`

#### `SCHEMA_REGISTRY_STENCIL_URLS`

Defines the stencil url. Multiple URLs could be given in a comma separated format.

* Example value: `http://localhost:8000/testproto.desc`
* Type: `required`

#### `SCHEMA_REGISTRY_STENCIL_REFRESH_CACHE`

Enable/Disable the stencil refresh cache.

* Example value: `false`
* Type: `optional`
* Default value: `false`

#### `SCHEMA_REGISTRY_STENCIL_TIMEOUT_MS`

Defines the stencil timeout in millis.

* Example value: `60000`
* Type: `optional`
* Default value: `60000`

### Flink

#### `FLINK_PARALLELISM`

Defines the number of flink parallelism.

* Example value: `1`
* Type: `optional`
* Default value: `1`

#### `FLINK_WATERMARK_INTERVAL_MS`

Defines the flink watermark interval in millis.

* Example value: `10000`
* Type: `optional`
* Default value: `10000`

#### `FLINK_WATERMARK_DELAY_MS`

Defines the flink watermark delay in millis.
* Example value: `10000`
* Type: `optional`
* Default value: `10000`

#### `FLINK_WATERMARK_PER_PARTITION_ENABLE`

Enable/Disable flink watermark per partition.

* Example value: `false`
* Type: `optional`
* Default value: `false`

#### `FLINK_CHECKPOINT_INTERVAL_MS`

Defines the flink checkpoint interval in millis.

* Example value: `30000`
* Type: `optional`
* Default value: `30000`

#### `FLINK_CHECKPOINT_TIMEOUT_MS`

Defines the flink checkpoint timeout in millis.

* Example value: `900000`
* Type: `optional`
* Default value: `900000`

#### `FLINK_CHECKPOINT_MIN_PAUSE_MS`

Defines the minimal pause between checkpointing attempts in millis.

* Example value: `5000`
* Type: `optional`
* Default value: `5000`

#### `FLINK_CHECKPOINT_MAX_CONCURRENT`

Defines the maximum number of checkpoint attempts that may be in progress at the same time.

* Example value: `1`
* Type: `optional`
* Default value: `1`

#### `FLINK_RETENTION_MIN_IDLE_STATE_HOUR`

Defines a a minimum time interval for how long idle state in hour.

* Example value: `8`
* Type: `optional`
* Default value: `8`

#### `FLINK_RETENTION_MAX_IDLE_STATE_HOUR`

Defines a a maximum time interval for how long idle state in hour.

* Example value: `9`
* Type: `optional`
* Default value: `9`

### Dart

Details of using Dart can be seen [here](../advance/DARTS.md).

#### `UDF_DART_GCS_PROJECT_ID`

Defines the gcs project id for Dart.

* Example value: `test-project`
* Type: `required`

#### `UDF_DART_GCS_BUCKET_ID`

Defines the gcs bucket id for Dart.

* Example value: `test-bucket`
* Type: `required`

### Longbow

Details of using Longbow can be seen [here](../advance/longbow.md).

#### `PROCESSOR_LONGBOW_ASYNC_TIMEOUT`

Defines the longbow async timeout.

* Example value: `15000L`
* Type: `optional`
* Default value: `15000L`

#### `PROCESSOR_LONGBOW_THREAD_CAPACITY`

Defines the longbow thread capacity.

* Example value: `30`
* Type: `optional`
* Default value: `30`

#### `PROCESSOR_LONGBOW_GCP_PROJECT_ID`

Defines the gcp project id for longbow.

* Example value: `test-longbow-project`
* Type: `required`

#### `PROCESSOR_LONGBOW_GCP_INSTANCE_ID`

Defines the gcp instance id for longbow.

* Example value: `test-longbow-instance`
* Type: `required`

#### `PROCESSOR_LONGBOW_GCP_TABLE_ID`

Defines the gcp table id for longbow.

* Example value: `test-longbow-table`
* Type: `required`

#### `PROCESSOR_LONGBOW_DOCUMENT_DURATION`

Defines the longbow document duration.

* Example value: `90d`
* Type: `optional`
* Default value: `90d`

will get `FLINK_JOB_ID` value for this config.

### PreProcessor

#### `PROCESSOR_PREPROCESSOR_ENABLE`

Enable/Disable using pre processor.

* Example value: `false`
* Type: `optional`
* Default value: `false`

#### `PROCESSOR_PREPROCESSOR_CONFIG`

Details on this configuration can be seen on [advance documentation](../advance/pre_processor.md) of pre processor.

### PostProcessor

#### `PROCESSOR_POSTPROCESSOR_ENABLE`

Enable/Disable using post processor.

* Example value: `false`
* Type: `optional`
* Default value: `false`

#### `PROCESSOR_POSTPROCESSOR_CONFIG`

Details on this configuration can be seen on [advance documentation](../advance/post_processor.md) of post processor.

### Telemetry

#### `METRIC_TELEMETRY_ENABLE`

Enable/Disable the flink telemetry for metric collection

* Example value: `true`
* Type: `optional`
* Default value: `true`

#### `METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS`

Shutdown period of metric telemetry in millis.

* Example value: `10000`
* Type: `optional`
* Default value: `10000`