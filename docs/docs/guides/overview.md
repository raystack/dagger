# Overview

The following section describes how to manage Dagger throughout its lifecycle.

### [Dagger Quickstart](./quickstart.md)

Get quickly up and running by setting up a local Dagger instance consuming from Kafka.

### [Choosing a Dagger Source](./choose_source.md)

Dagger requires configuring a source from where data will be streamed for processing. This section explains what are the
different available sources.

### [Creating Dagger](./create_dagger.md)

Dagger currently supports InfluxDB, Kafka and BigQuery as supported sinks. This section explains
how you can create a dagger and configure different settings.

### [Deploying Dagger](./deployment.md)

Dagger runs inside a Flink cluster which can be set up in some distributed resource managers like YARN, VMs or containers in a fully managed runtime environment like Kubernetes. This section contains guides, best practices and advice related to deploying Dagger in production.

### [Monitoring Dagger with exposed metrics](./monitoring.md)

Dagger support first-class monitoring support. You can get a lot of insights about a running dagger from the pre-exported [monitoring dashboards](https://github.com/raystack/dagger/blob/main/docs/static/assets/dagger-grafana-dashboard.json). This section contains guides, best practices and pieces of advice related to managing Dagger in production.

### [Query Examples](./query_examples.md)

This section contains examples of few widely used Dagger SQL queries.

### [Troubleshooting Dagger](./troubleshooting.md)

Dagger scales in an instant, both vertically and horizontally to adhere to high throughput data processing. In addition to this Flink's inbuilt fault-tolerance mechanism ensures zero data drops.
However, a complex distributed data processing application can fail due to many reasons.
This section contains guides, best practices and bits of advice related to troubleshooting issues/failures with Dagger in production.

### [Use UDFs](./use_udf.md)

Explains how to use User Defined Functions(UDFs) in Dagger SQL queries.

### [Use Transformer](./use_transformer.md)

Transformers are Dagger specific plugins where user can specify custom Operators and inject them in Dagger. This section talks briefly about how to define and use a Transformer in Dagger.
