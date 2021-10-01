# Metrics

[Will be updating it once the new metrics are onboard]

Service-level Indicators \(SLIs\) are the measurements used to calculate the performance for the goal. It is a direct measurement of a service’s behaviour and helps us and the users to evaluate whether our system has been running within SLO. The metrics captured as part of SLI for Dagger are described below.

## Table of Contents

- [Overview](metrics.md#overview)
- [Kafka Consumer Details](metrics.md#kafka-consumer-details)
- [Input Stream](metrics.md#input-stream)
- [Exceptions](metrics.md#exceptions)
- [Kafka Producer](metrics.md#kafka-producer)
- [Output Stream](metrics.md#output-stream)
- [UDFs](metrics.md#udfs)
- [Processors](metrics.md#processors)
- [Longbow](metrics.md#longbow)
- [Checkpointing](metrics.md#checkpointing)

## Overview

Some of the most important metrics related to a dagger that gives you an overview of the current state of it.

### `Status`

- Status of the job on the cluster. Could be either of these three.
  `Stopped`: Current status is stopped
  `Started`: The dagger has restarted once or more during the selected time interval `Running`: The dagger is running fine for the duration selected

### `Runtime since last restart`

- The time that the job ran/running since the last restart.

### `Sink`

- The configured sink type for dagger. Can be Log/Kafka/Influx.

### `Full restarts`

- The total number of full restarts since this job was submitted.

### `Total downtime`

- For jobs currently in a restarting situation, the time elapsed during this outage. Returns 0 for running jobs.

### `Max kafka consumer lag`

- The maximum lag in terms of number of records for any partition in this window. An increasing value over time is your best indication that the consumer group is not keeping up with the producers.

### `Late Records Dropped per 5 minutes`

- The number of records a task has dropped due to arriving late.

### `Records Rate - All Stages`

- This gives an idea of data flow through tasks. `Input`: The number of records a task receives per second. `Output`: The number of records a task sends per second.

## Kafka Consumer Details

This lists all the Kafka consumer level information.

### `Assigned partitions`

- Consumer Group Metrics: The number of partitions currently assigned to this consumer (per source task). 1 <= Number of task <= parallelism.

### `Consumer Number of Request /second`

- Global Request Metrics: The average number of requests sent per second per source task.

### `Records Consumed Rate /second`

- The average number of records consumed per second per source task (a task that is responsible for reading from Kafka).

### `Bytes Consumed Rate /second`

- The average number of bytes consumed per second per source task (part of the job responsible for consuming from Kafka) .

### `Fetch Rate /second`

- Fetch Metrics: The number of fetch requests per second per source task(part of the job responsible for consuming from Kafka).

### `Max Fetch Latency`

- Fetch Metrics: The max time taken for a fetch request per source task(part of the job responsible for consuming from Kafka).

### `Avg Fetch Latency`

- The average time taken for a fetch request per source task(part of the job responsible for consuming from Kafka).

### `Avg Fetch Size`

- The average number of bytes fetched per request per source task(part of the job responsible for consuming from Kafka).

### `Max Fetch Size`

- The max number of bytes fetched per request per source task(part of the job responsible for consuming from Kafka).

### `Commit Rate /second`

- The number of commit calls per second per source task(part of the job responsible for consuming from Kafka).

### `Consumer Active Connections Count`

- The current number of active connections per source task(part of the job responsible for consuming from Kafka).

### `New Connections Creation Rate /second`

- New connections established per second in the window per source task(part of the job responsible for consuming from Kafka)

### `Connections Close Rate /second`

- Connections closed per second in the window per source task(part of the job responsible for consuming from Kafka).

### `Consumer Outgoing Byte Rate/second`

- The average number of outgoing bytes sent per second to all servers per source task(part of the job responsible for consuming from Kafka).

### `Sync rate /second`

- The number of group syncs per second per source task(part of the job responsible for consuming from Kafka) . Group synchronization is the second and last phase of the rebalance protocol. Similar to join-rate, a large value indicates group instability .

### `Avg Commit Latency`

- The average time taken for a commit request per source task(part of the job responsible for consuming from Kafka).

### `Max Commit Latency`

- The max time taken for a commit request per source task(part of the job responsible for consuming from Kafka).

### `Consumer Network IO rate /second`

- The average number of network operations (reads or writes) on all connections per second per source task(part of the job responsible for consuming from Kafka).

## Input Stream

Input Kafka level information.

### `Input topics`

- Lists of Kafka topics from which dagger is consuming data.

### `Input protos`

- The protos which the dagger is using to consume from topics.

## Exceptions

This panel shows details about exceptions/unusual behaviors in a running/failing dagger.

### `Fatal Exceptions`

- List of exceptions thrown by dagger which will result in restarting the job.

### `Warning`

- List of warnings coming in dagger(not resulting in job restart).

### `InfluxDB late record Drops`

- The number of records that could not be pushed to InfluxDB due to out of bound retention period. This is a counter so is incremental for a given job.

## Kafka Producer

This panel show kafka producer level information. The metrics here are for Kafka Sink dagger.

### `Avg Request Latency`

- Avg time between when send() was called by the producer until the producer receives a response from the broker per source task(part of the job responsible for producing to Kafka).

### `Max Request Latency`

- Max time between when send() was called by the producer until it receives a response from the broker per source task(part of the job responsible for producing to Kafka).

### `Produce Request Rate /second`

- The average number of requests sent per second per source task(part of the job responsible for producing to Kafka)

### `Produce Response Rate /second`

- The average number of responses received per second per source task(part of the job responsible for producing to Kafka)

### `Record Send Rate /second`

- The average number of records sent for a topic per second per source task (a task that is responsible for producing to Kafka).

### `Average Records Per Request`

- The average number of records in each request per source task (a task that is responsible for producing to Kafka).

### `Producer Active Connection Count`

- The current number of active connections per source task (a task that is responsible for producing to Kafka).

### `Producer Network IO rate /second`

- The average number of network operations (reads or writes) on all connections per second per source task (a task that is responsible for producing to Kafka).

### `Avg Record Size`

- The average record size per source task (a task that is responsible for producing to Kafka).

### `Max Record Size`

- The maximum record size per source task (a task that is responsible for producing to Kafka).

### `Average Record Send Error Rate /second`

- The average number of records sent that resulted in errors for a topic per-second per source task (a task that is responsible for producing to Kafka).

### `Requests In Flight`

- Number of in-flight requests awaiting a response per source task (a task that is responsible for producing to Kafka).

### `Avg Record Queue Time`

- The average time in milliseconds record batches spent in the record accumulator per source task (a task that is responsible for producing to Kafka).

### `Max Record Queue Time`

- The maximum time in milliseconds record batches spent in the record accumulator per source task (a task that is responsible for producing to Kafka).

## Output Stream

This is specific to Kafka Sink Daggers and shows some of the information related to out put kafka.

### `Output Topic`

- The output topic in which Dagger will be producing Data.

### `Output Proto`

- The proto which the dagger is using to produce to the topic. This is applicable for only Kafka sink daggers.

## UDFs

UDFs or User Defined Functions are the way to add functions to be used as SQL in Flink Query. This sections shows detailed metrics about some UDFs.

### `Names`

- List of UDFs used in the current dagger query. Find more on supported UDFS [here](../reference/udfs.md).

### `Darts: Metrics`

- Path and Size of the file used in DART from GCS.

### `Darts: Cache hit`

- The average rate of the cache hit when using DARTs.

### `Darts: Cache miss`

- The average rate of the cache miss when using DARTs.

### `Darts: GCS fetch success`

- The average rate of the successful GCS fetch when using DARTs.

### `Darts: GCS fetch failure`

- The average rate of the failure GCS fetch when using DARTs.

## Processors

Processors are the other way of adding plugins to dagger. With the help of them you can talk to external data source or inject custom code to dagger. This section shows all the metrics related to pre and post processors. Find more info about them [here](../advance/overview).

### `Pre Processors Names`

- List of Pre Processors applied on the dagger.

### `Post Processors Names`

- List of Post Processors applied on the dagger.

### `Total External calls rate`

- Number of Successful events received per minute

### `Success Response time`

- Time taken by the external client to send back the response to daggers, in case of a successful event.

### `Success Rate`

- Total calls to the external source per minute. which can be either one of an HTTP endpoint / ElasticSearch / PostgresDB, based on post processor type.

### `Failure Response Time`

- Time taken by the external client to send back the response to daggers, in case of a failed event.

### `Total Failure Request Rate`

- Number of failed events received per minute

### `Failure Rate on 404 response`

- Number of failed events received per minute with response code 404 (Not Found)

### `Failure Rate on 4XX Response`

- Number of failed events received per minute with response code 4XX , other than 404

### `Failure Rate on 5XX Response`

- Number of failed events received per minute with response code 5XX

### `Timeouts rate`

- Number of timeouts per minute from external source, which can be either one of an HTTP endpoint / ElasticSearch / PostgresDB, based on post processor type

### `Close connection on client calls`

- Number of times connection to the external client is closed per minute

### `Failure Rate on Reading path`

- Number of failed events per minute caused while reading path configured in post processor config

### `Failure Rate on Other errors`

- Failure rate due to an error other than the ones covered above

### `Failure Rate on Empty Request`

- Failure rate when all the values of the request variable are empty or empty endpoints.

### `Records Dropped`

- Rate of dropped records due to failures.

## Longbow

These dashboards show the details about Longbow (Long window daggers). They can be either related to Longbow Read or Longbow Writes. Find more details about longbow [here](../advance/longbow.md).

### `Rate of successful reads`

- Number of successfully read documents from Big Table per second.

### `Successful read time`

- Time taken by the Big Table to send back the response to daggers, in case of a successful read event.

### `Successful reads`

- Number of successfully read documents from Big Table per 10s.

### `Docs read per scan`

- Number of documents read from Big Table per scan.

### `Missed Records`

- Indicates those events for which there is either no data or invalid data in the Big Table.

### `Failed to Read Documents`

- Number of failures in reading documents.

### `Time Taken for Failed Documents`

- Time taken by the Big Table to send back the response to daggers, in case of a failed read event.

### `Timeout`

- Number of timeouts from Big Table while reading from it.

### `Close Connection`

- Number of times connection to the Big Table is closed while reading from it.

### `Rate of Successful Writes`

- Number of successful writes to Big Table per second.

### `Successful write time`

- Time taken by the Big Table to send back the response to daggers, in case of a successful write event.

### `Successful Document writes`

- The number of successful document writes to Big Table per 10s.

### `Failed to write documents`

- Number of failures in writing documents to Big Table.

### `Time taken for Failed documents`

- Time taken by the Big Table to send back the response to daggers, in case of a failed write event.

### `Write timeouts`

- Number of timeouts from Big Table while writing to it.

### `Write Close Connection`

- Number of times connection to the Big Table is closed while writing to it.

### `Create bigtable`

- Indicates whether a table was created successfully in Big Table or not.

## Checkpointing

This shows the details about checkpointing of the dagger job. Checkpointing is Flink's internal way of taking snapshots.

### `Number of completed checkpoints`

- Checkpoints successfully completed by the job. With time the number of checkpoints will increase.

### `Number of failed checkpoints`

- The number of failed checkpoints.

### `Last Checkpoint Duration in Sec`

- Time taken for the job manager to do the last checkpointing in Seconds.

### `Last Checkpoint Size`

- The total size of the last checkpoint.

### `Status`

- This shows the health of the job every minute. In case the job is not running fine you would see a red bar.

### `Team`

- Team who has the ownership for the given firehose.

### `Proto Schema`

- The proto class used for creating the firehose

### `Stream`

- The stream where the input topic is read from

## Overview

Some of the most important metrics related to firehose that gives you an overview of the current state of it.

### `ConsumerLag: MaxLag`

- The maximum lag in terms of number of records for any partition in this window. An increasing value over time is your best indication that the consumer group is not keeping up with the producers.

### `Total Message Received`

- Sum of all messages received from Kafka per pod.

### `Message Sent Successfully`

- Messages sent successfully to the sink per batch per pod.

### `Message Sent Failed`

- Messages failed to be pushed into the sink per batch per pod. In case of HTTP sink, if status code is not in retry codes configured, the records will be dropped.

### `Message Dropped`

- In case of HTTP sink, when status code is not in retry codes configured, the records are dropped. This metric captures the dropped messages count.

### `Batch size Distribution`

- 99p of batch size distribution for pulled and pushed messages per pod.

### `Time spent in firehose`

- Latency introduced by firehose \(time before sending to sink - time when reading from Kafka\). Note: It could be high if the response time of the sink is higher as subsequent batches could be delayed.

### `Time spent in pipeline`

- Time difference between Kafka ingestion and sending to sink \(Time before sending to sink - Time of Kafka ingestion\)

### `Sink Response Time`

- Different percentile of the response time of the sink.

## Pods Health

Since firehose runs on Kube, this gives a nice health details of each pods.

### `JVM Lifetime`

- JVM Uptime of each pod.

### `Cpu Load`

- Returns the "recent cpu usage" for the Java Virtual Machine process. This value is a double in the \[0.0,1.0\] interval. A value of 0.0 means that none of the CPUs were running threads from the JVM process during the recent period of time observed, while a value of 1.0 means that all CPUs were actively running threads from the JVM 100% of the time during the recent period being observed. Threads from the JVM include the application threads as well as the JVM internal threads. All values betweens 0.0 and 1.0 are possible depending of the activities going on in the JVM process and the whole system. If the Java Virtual Machine recent CPU usage is not available, the method returns a negative value.

### `Cpu Time`

- Returns the CPU time used by the process on which the Java virtual machine is running. The returned value is of nanoseconds precision but not necessarily nanoseconds accuracy.

## Kafka Consumer Details

Listing some of the Kafka consumer metrics here.

### `Assigned partitions`

- Consumer Group Metrics: The number of partitions currently assigned to this consumer \(per pod\).

### `Consumer Number of Request/second`

- Global Request Metrics: The average number of requests sent per second per pod.

### `Records Consumed Rate/second`

- Topic-level Fetch Metrics: The average number of records consumed per second for a specific topic per pod.

### `Bytes Consumed Rate/second`

- Topic-level Fetch Metrics: The average number of bytes consumed per second per pod.

### `Fetch Rate/second`

- Fetch Metrics: The number of fetch requests per second per pod.

### `Max Fetch Latency`

- Fetch Metrics: The max time taken for a fetch request per pod.

### `Average Fetch Latency`

- Fetch Metrics: The average time taken for a fetch request per pod.

### `Average Fetch Size`

- Fetch Metrics: The average number of bytes fetched per request per pod.

### `Max Fetch Size`

- Fetch Metrics: The max number of bytes fetched per request per pod.

### `Commit Rate/second`

- Consumer Group Metrics: The number of commit calls per second per pod.

### `Consumer Active Connections Count`

- Global Connection Metrics: The current number of active connections per pod.

### `New Connections Creation Rate/second`

- Global Connection Metrics: New connections established per second in the window per pod.

### `Connections Close Rate/second`

- Global Connection Metrics: Connections closed per second in the window per pod.

### `Consumer Outgoing Byte Rate/Sec`

- Global Request Metrics: The average number of outgoing bytes sent per second to all servers per pod.

### `Avg time between poll`

- Average time spent between poll per pod.

### `Max time between poll`

- Max time spent between poll per pod.

### `Sync rate`

- Consumer Group Metrics: The number of group syncs per second per pod. Group synchronization is the second and last phase of the rebalance protocol. Similar to join-rate, a large value indicates group instability.

### `Consumer Network IO rate /second`

- The average number of network operations \(reads or writes\) on all connections per second per pod

### `Rebalance Rate /hour`

- Rate of rebalance the consumer.

### `Average Commit latency`

- Consumer Group Metrics: The average time taken for a commit request per pod

### `Max Commit latency`

- Consumer Group Metrics: The max time taken for a commit request per pod.

### `Avg Rebalance latency`

- Average Rebalance Latency for the consumer per pod.

### `Max Rebalance latency`

- Max Rebalance Latency for the consumer per pod.

## Error

This gives you a nice insight about the critical and noncritical exceptions happened in the firehose.

### `Fatal Error`

- Count of all the exception raised by the pods which can restart the firehose.

### `Nonfatal Error`

- Count of all the exception raised by the firehose which will not restart the firehose and firehose will keep retrying.

## Memory

Details on memory used by the firehose for different tasks.

### `Heap Memory Usage`

- Details of heap memory usage:

  ```text
  Max: The amount of memory that can be used for memory management
  Used: The amount of memory currently in use
  ```

### `Non-Heap Memory Usage`

- Details of non-heap memory usage:

  ```text
  Max: The amount of memory that can be used for memory management
  Used: The amount of memory currently in use
  ```

### `GC: Memory Pool Collection Usage`

- For a garbage-collected memory pool, the amount of used memory includes the memory occupied by all objects in the pool including both reachable and unreachable objects. This is for all the names in the type: MemoryPool.

### `GC: Memory Pool Peak Usage`

- Peak usage of GC memory usage.

### `GC: Memory Pool Usage`

- Total usage of GC memory usage.

## Garbage Collection

All JVM Garbage Collection Details.

### `GC Collection Count`

- The total number of collections that have occurred per pod. Rather than showing the absolute value we are showing the difference to see the rate of change more easily.

### `GC Collection Time`

- The approximate accumulated collection elapsed time in milliseconds per pod. Rather than showing the absolute value we are showing the difference to see the rate of change more easily.

### `Thread Count`

- daemonThreadCount: Returns the current number of live daemon threads per pod peakThreadCount: Returns the peak live thread count since the Java virtual machine started or peak was reset per pod threadCount: Returns the current number of live threads including both daemon and non-daemon threads per pod.

### `Class Count`

- loadedClass: Displays number of classes that are currently loaded in the Java virtual machine per pod unloadedClass: Displays the total number of classes unloaded since the Java virtual machine has started execution.

### `Code Cache Memory after GC`

- The code cache memory usage in the memory pools at the end of a GC per pod.

### `Compressed Class Space after GC`

- The compressed class space memory usage in the memory pools at the end of a GC per pod.

### `Metaspace after GC`

- The metaspace memory usage in the memory pools at the end of a GC per pod.

### `Par Eden Space after GC`

- The eden space memory usage in the memory pools at the end of a GC per pod.

### `Par Survivor Space after GC`

- The survivor space memory usage in the memory pools at the end of a GC per pod.

### `Tenured Space after GC`

- The tenured space memory usage in the memory pools at the end of a GC per pod.

  **`File Descriptor`**

- Number of file descriptor per pod

  ```text
  Open: Current open file descriptors
  Max: Based on config max allowed
  ```

## Retry

If you have configured retries this will give you some insight about the retries.

### `Average Retry Requests`

- Request retries per min per pod.

### `Back Off time`

- Time spent per pod backing off.

## HTTP Sink

HTTP Sink response code details.

### `2XX Response Count`

- Total number of 2xx response received by firehose from the HTTP service,

### `4XX Response Count`

- Total number of 4xx response received by firehose from the HTTP service.

### `5XX Response Count`

- Total number of 5xx response received by firehose from the HTTP service.

### `No Response Count`

- Total number of No response received by firehose from the HTTP service.

## Filter

Since firehose supports filtration based on some data, these metrics give some information related to that.

### `Filter Type`

- Type of filter in the firehose. It will be one of the "none", "key", "message".

### `Total Messages filtered`

- Sum of all the messages filtered because of the filter condition per pod.
