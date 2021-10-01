# Monitoring

For a complex application like Dagger monitoring plays an important role.
Monitoring goes hand-in-hand with observability, which is a prerequisite for troubleshooting and performance tuning.
This section will give a brief overview of Dagger's monitoring stack and explain some important panels from the pre-built dashboards which might help you in running your dagger in production.

## Metrics Stack

We use Flink's inbuilt metrics reporter to publish application metrics to one of the supported sinks. Other metrics like JMX can be enabled from Flink. Find more details on Flink's metrics reporting and supported sinks [here](https://ci.apache.org/projects/flink/flink-docs-release-1.9/monitoring/metrics.html#reporter).
To register new application metrics from the dagger codebase follow [this](https://ci.apache.org/projects/flink/flink-docs-release-1.9/monitoring/metrics.html#registering-metrics/).

We have also included a [custom grafana dashboard](https://github.com/odpf/dagger/blob/main/docs/static/assets/dagger-grafana-dashboard.json) for dagger related [metrics](../reference/metrics.md).
Follow [this](https://grafana.com/docs/grafana/latest/dashboards/export-import/) to import this dashboard.

## Dagger Dashboard

This section gives an overview of the important panels/titles in the dagger [dashboard](https://github.com/odpf/dagger/blob/main/docs/static/assets/dagger-grafana-dashboard.json).
Find more about all the panels [here](../reference/metrics.md).

#### Overview

It shows the basic status of a Dagger which is kind of self-explanatory like running since, number of restarts, downtime etc.

Listing some other important panels in this section below.
`Max Kafka Consumer Lag`: shows the Consumer Lag of the Kafka consumer.

`Late Records Dropped`: The number of records a task has dropped due to arriving late.

`Records Rate`: Input - The number of records the Dagger receives per second. Output - The number of records a task sends per second.

#### Kafka Consumer

All the metrics related to the Kafka Consumer. Helps you debug the issues from the consumer side. Some of the important panels here are

`Records Consumed Rate /second` The average number of records consumed per second.

`Bytes Consumed Rate /second` The average size of records consumed per second.

`Commit Rate /second` The average number of records committed per second. etc.

#### Input Stream

Shows some basic information about input Kafka Streams like Topics, Proto and Kafka clusters etc.

#### Exception

Shows the warning/errors which would be causing issues in the Dagger jobs.
`Fatal Exceptions` cause the job to restart.
`Warnings` are potential issues but don’t restart the job.

#### Output Stream

Shows some basic information about output Kafka Stream like Topics, Proto and Kafka cluster etc. Only gets populated for Kafka sink Dagger.

#### UDF

Lists the Name of the UDFs in the SQL query. Also, have the Dart related metrics for a Dart Dagger.

#### Post Processors

Lists all the Post Processors in a Dagger and some of the crucial metrics related to Post Processors.

`Total External Calls Rate` total calls to the external source per minute. Which can be either one of an HTTP endpoint / ElasticSearch / PostgresDB.

`Success Response Time` Time taken by the external client to send back the response to Dagger, in case of a successful event.

`Success Rate` Number of successful events per minute.

`Total Failure Request Rate` Number of failed events per minute.

`Failure Rate on XXX calls` Number of failed events per minute due to XXX response code.

`Timeouts Rate` Number of timeouts per minute from an external source, which can be either one of an HTTP endpoint / ElasticSearch / PostgresDB, based on Post Processor type.

`Close Connection On Client Rate` Number of times connected to the external client is closed per minute.

#### Longbow

Lists some important information related to the Longbow Read and Write Dagger.

#### Checkpointing

To make the state fault-tolerant, Flink needs to checkpoint the state. Checkpoints allow Flink to recover state and positions in the streams to give the application the same semantics as a failure-free execution. A Dagger job periodically checkpoints to a GCS bucket. This section has all the checkpointing details.
`Number of checkpoints` total of checkpoints triggered since the Dagger has been started.

`Number of failed checkpoints` total number of failed checkpoints since the Dagger started. If the failed checkpoint number is high, the Dagger would also restart.

`Number of in-progress checkpoints` number of checkpoints currently in progress.

`Last checkpoint size` is the total size of the state that needs to be checkpointed. For a larger state size (> 500 MB) one needs to increase the parallelism.
