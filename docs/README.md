# Introduction

Dagger is a cloud native service for delivering real-time streaming data to destinations such as service endpoints (HTTP or GRPC) & managed databases (Postgres, InfluxDB,  Redis, & Elasticsearch). With Dagger, you don't need to write applications or manage resources. It can be scaled up to match the throughput of your data. If your data is present in Kafka, Dagger delivers it to the destination(SINK) that you specified.

![](.//assets/overview.svg)

## Key Features
Discover why users choose Dagger as their main Kafka Consumer

* **Extensibility:** Add your own sink to dagger with a clearly defined interface or choose from already provided ones.
* **Metrics:** Always know what’s going on with your deployment with built-in [monitoring](./docs/assets/dagger-grafana-dashboard.json) of throughput, response times, errors and more.

To know more, follow the detailed [documentation](docs)

## Usage

Explore the following resources to get started with Dagger:

* [Guides](docs/guides) provides guidance on [creating Dagger](docs/guides/overview.md) with different sinks.
* [Concepts](docs/concepts) describes all important Dagger concepts.
* [Reference](docs/reference) contains details about configurations, metrics and other aspects of Dagger.
* [Contribute](docs/contribute/contribution.md) contains resources for anyone who wants to contribute to Dagger.

