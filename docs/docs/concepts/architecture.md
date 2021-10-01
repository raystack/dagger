# Architecture

Dagger or Data Aggregator is a cloud native framework for processing real-time streaming data built on top of Apache Flink.

## System Design

### Components

![Dagger Architecture](/img/architecture.png)

_**Stream**_

- Stream are logical Data sources from which Dagger reads unbounded Data.
- Dagger only supports Kafka Queue as the supported data source for now. In the context of Kafka, a single stream is a group of Kafka topics with the same schema. Dagger can support multiple streams at a time.
- Dagger consumes the data points from Kafka. So many Kafka consumer-level configurations like consumer groups and auto offset reset can be set in the stream itself.

_**Dagger Core**_

- The core part of the dagger(StreamManager) has the following responsibilities. It works sort of as a controller for other components in the dagger.
  - Configuration management.
  - Table registration.
  - Configuring Deserialization and Serialization of data.
  - Manage execution of other plugins like pre and post processors.
  - User-Defined functions registration.

_**Pre-Processor**_

- Each stream registered on daggers can have chained pre-processors. They will run and transform the table before registering it for SQL.
- Currently, only [transformers](../guides/use_transformer.md) are supported as part of the pre-processor. Pre-processor are simply Flink's ProcessFunctions/Operators which can transform the input stream to another stream of data prior to execution of SQL. They are ideal for complex filtration logic.

_**SQL Execution**_

- Dagger uses Flink's native SQL Planner to executed SQL(The more powerful Blink Planner will be supported in the next releases).
- In this layer dagger executes streaming SQL (similar to ANSI SQL) on registered unbounded DataStream(s). Flink uses apache calcite for SQL execution.
- In case you have registered any [UDF](../guides/use_udf.md), SQL engines picks them up and lets you use them in your defined SQL query.

_**ProtoHandlers**_

- Proto handler handles the SerDe mechanism for protobuf data to Flink understandable Distributed Data format(Flink Row).
- It recursively parses complex Kafka messages to Flink Row on the consumer side and Flink row to Kafka messages on the producer side.
- It does handle the Type conversion of data points.

_**Post-Processor**_

- Post-processors are similar to pre-processors but are rich in functionalities.
- They also enable async processing from external Data source/Service endpoints over network calls and complex transformation beyond SQL.
- They use some of the advanced Flink functionalities like RichFunctions and RichAsyncFunction as the building block. User can define a JSON based DSL to interact with them.
- Some of the supported types of post processors are
  - [External Post-Processors](../advance/post_processor.md#external-post-processor)
  - [Longbow](../advance/longbow.md)
  - [Internal Post-Processors](../advance/post_processor.md#internal-post-processor)
  - [Transformers](../guides/use_transformer.md)

_**Telemetry Processor**_

- For a distributed streaming engine like Dagger, it becomes really essential to collect and report all the essential application-level metrics. The Telemetry Processor is responsible to do so.
- In its core Telemetry Processor is another type of post-processor which internally use pub-sub to collect application metrics.
- After the metrics are collected they need to be sent to some external systems. Metrics reporter which is a library in the Flink cluster does this. Follow [this](https://ci.apache.org/projects/flink/flink-docs-release-1.9/monitoring/metrics.html) for more detailed information about flink metrics.

_**Sink and Serializer**_

- After the data is processed and results are materialized they need to be sinked to some external persistent storage.
- Dagger supports Kafka and InfluxDB as supported sinks where the unbounded results are pushed at the end of the lifecycle.
- In the case of Kafka Sink the final result is protobuf encoded. So the result goes through a serialization stage on some defined output schema. The serializer module of the proto-handler does this. Results in Kafka can be used via any Kafka consumer.
- Influx Sink helps in real-time analytics and dashboarding. In the case of Influx Sink dagger, converts results in Flink Row to InfluxDB points and add `tag`/`labels` as specified in the SQL.

### Schema Handling

- Protocol buffers are Google's language-neutral, platform-neutral, extensible mechanism for serializing structured data. Data streams on Kafka topics are bound to a protobuf schema.

- Dagger deserializes the data consumed from the topics using the Protobuf descriptors generated out of the artifacts. The schema handling ie., find the mapped schema for the topic, downloading the descriptors, and dynamically being notified of/updating with the latest schema is abstracted through a homegrown library called [stencil](https://github.com/odpf/stencil).

- Stencil is a proprietary library that provides an abstraction layer, for schema handling.

- Schema Caching, dynamic schema updates are features of the stencil client library.

## Dagger Integration

### Kafka Input

- The Kafka topic\(s\) where Dagger reads from.

### ProtoDescriptors

- Generated protobuf descriptors which are hosted behind an artifactory/HTTP endpoint. This endpoint URL and the proto that the dagger deployment should use to deserialize data can be configured.

### Flink Job Manager

- JobManager is the JVM process that acts as the brain of Flink's execution life cycle. Handles SQL execution, Job Graph creation, Coordinate Task Managers, Snapshotting and many other critical operations for distributed stream processing.

### Flink Task Manager

- Taskmanagers (TM) are the JVM processes which are actually responsible for parallel stateful stream processing of unbounded data. Taskslots are single threads inside Flink's Taskmanager that process single operators in the job graph.

### Zookeeper

- Flink leverages ZooKeeper for distributed coordination between all running JobManager instances. ZooKeeper is a separate service from Flink, which provides highly reliable distributed coordination via leader election and lightweight consistent state storage.

### Object Store

- Operators in Flink can be stateful. In order to make state fault-tolerant, Flink needs to checkpoint the state in some sort of distributed persistent object stores like s3/GCS/HDFS. Checkpoints allow Flink to recover state and positions in the streams to give the application the same semantics as a failure-free execution.

### Metrics Reporter

- Metrics reporter sends the application metrics to some other external systems optimised for metrics data. Metrics reporters can be configured at the cluster level.

### Sink

- InfluxDB - time-series database for real-time analytics.
- Kafka - Replayable queue to easy use of generated results.
