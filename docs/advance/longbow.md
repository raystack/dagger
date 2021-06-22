# Introduction
This is another type of [Post Processor](update link) i.e. it is also applied post SQL query processing in the Dagger work flow. We currently use Flink's [FsStateBackend](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/state/state_backends.html#the-fsstatebackend). We observed that for use cases where aggregation window was larger i.e in days or months, Dagger jobs required a lot of resources in order to maintain the state. Hence we created a solution where we moved the entire state from Dagger's memory to an external data store. After evaluating a lot of data sources we found [Bigtable](https://cloud.google.com/bigtable) to be a good fit primarily because of it's low scan queries latencies.

# Components
In order to make this work in a single Dagger job, we created following components.
* [Longbow writer](longbow.md#longbow_writer)
* [Longbow reader](longbow.md#longbow_reader)

## Longbow writer
This component is responsible for writing the latest data to Bigtable. It uses Flink's [Async IO](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/operators/asyncio.html) in order to make this network call.

### Workflow
* Create a new table(if doesn't exist) with the name same as Dagger job name.
* Receives the record post SQL query processing.
* Creates the Bigtable key by combining data from longbow_key, a delimeter and reversing the event_timestamp. Timestamps are reversed in order to acheive lower latencies in scan query, more details [here](https://cloud.google.com/bigtable/docs/schema-design#time-based).


## Longbow reader
This component is responsible for reading the historical data from Bigtable and forwarding it to the sink. It also uses Flink's [Async IO](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/operators/asyncio.html) in order to make this network call.