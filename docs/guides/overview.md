# Overview

The following section describes how to manage Dagger throughout its lifecycle. 

### Creating Dagger
Dagger currently supports InfluxDB, Kafka as supported sinks. This section explains
how you can create a dagger and configure different settings.

### Deploying Dagger

Dagger runs inside a Flink cluster which can be set up in some distributed resource managers like YARN 
VMs or containers in a fully managed runtime environment like Kubernetes. This section contains guides, best practices and advices related to deploying Dagger in production.

### Monitoring Dagger with exposed metrics 
Dagger support first class monitoring support. You can get a lot of insights about a 
running dagger from the pre-exported [monitoring dashboards](https://github.com/odpf/dagger/blob/main/docs/assets/firehose-grafana-dashboard.json) . This section contains guides, best practices and advices related to managing Dagger in production.

### Troubleshooting Dagger
Dagger scales in an instant, both vertically and horizontally to adhere to high throughput data processing. In addition to this Flink's inbuilt fault-tolerance mechanisms ensure zero data drops.
However a complex distributed data processing application can fail due to many reasons.
This section contains guides, best practices and advices related to troubleshooting issues/failures with Dagger in production.
