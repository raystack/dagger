# Dagger
![build workflow](https://github.com/odpf/dagger/actions/workflows/build.yml/badge.svg)
![package workflow](https://github.com/odpf/dagger/actions/workflows/package.yml/badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache)](LICENSE)
[![Version](https://img.shields.io/github/v/release/odpf/dagger?logo=semantic-release)](Version)

Dagger is a cloud native service for delivering real-time streaming data to destinations such as service endpoints (HTTP or GRPC) & managed databases (Postgres, InfluxDB,  Redis, & Elasticsearch). With Dagger, you don't need to write applications or manage resources. It can be scaled up to match the throughput of your data. If your data is present in Kafka, Dagger delivers it to the destination(SINK) that you specified.

<p align="center"><img src="./docs/assets/overview.svg" /></p>

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


## Contribute

Development of Dagger happens in the open on GitHub, and we are grateful to the community for contributing bugfixes and improvements. Read below to learn how you can take part in improving Dagger.

Read our [contributing guide](docs/contribute/contribution.md) to learn about our development process, how to propose bugfixes and improvements, and how to build and test your changes to Dagger.

To help you get your feet wet and get you familiar with our contribution process, we have a list of [good first issues](https://github.com/odpf/dagger/labels/good%20first%20issue) that contain bugs which have a relatively limited scope. This is a great place to get started.

This project exists thanks to all the [contributors](https://github.com/odpf/dagger/graphs/contributors).

## License
Dagger is [Apache 2.0](LICENSE) licensed.