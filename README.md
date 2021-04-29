# Dagger
![build workflow](https://github.com/odpf/dagger/actions/workflows/build.yml/badge.svg)
![package workflow](https://github.com/odpf/dagger/actions/workflows/package.yml/badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache)](LICENSE)
[![Version](https://img.shields.io/github/v/release/odpf/dagger?logo=semantic-release)](Version)

Dagger or Data Aggregator is a cloud native framework for processing real-time streaming data built on top of Apache Flink and Kubernetes. With Dagger, you don't need to write custom applications or manage resources to process data in real time. Instead simple SQL on input Data stream does the job. 

<p align="center"><img src="./docs/assets/overview.svg" /></p>

## Key Features
Discover why to use Dagger

* **Processing:** Dagger can transform, aggregate, join and enrich protobuf data in real-time.
* **Scale:** Dagger scales in an instant, both vertically and horizontally for high performance streaming sink and zero data drops.
* **Extensibility:** Add your own sink to dagger with a clearly defined interface or choose from already provided ones.
* **Pluggability:** Add custom bussiness logics in form puglins \(UDFs, Transformers, Preprocessors and Post Processors\) independent of core logic. 
* **Metrics:** Always know what’s going on with your deployment with built-in [monitoring](./docs/assets/dagger-grafana-dashboard.json) of throughput, response times, errors and more.

To know more, follow the detailed [documentation](docs)

## Usage

Explore the following resources to get started with Dagger:

* [Guides](docs/guides) provides guidance on [creating Dagger](docs/guides/overview.md) with different sinks.
* [Concepts](docs/concepts) describes all important Dagger concepts.
* [Reference](docs/reference) contains details about configurations, metrics and other aspects of Dagger.
* [Contribute](docs/contribute/contribution.md) contains resources for anyone who wants to contribute to Dagger.

## Running locally

```sh
# Clone the repo
$ git clone https://github.com/odpf/dagger.git  

# Build the jar
$ ./gradlew clean build 

# Configure env variables
$ cat env/local.properties

# Run a Dagger
$ ./gradlew runFlink
```
**Note:** Sample configuration for running a basic dagger can be found  [here](/docs/reference/configuration.md).

## Running tests 
```sh
# Running unit tests
$ ./gradlew clean test

# Run code quality checks
$ ./gradlew checkstyleMain checkstyleTest

#Cleaning the build
$ ./gradlew clean
```

## Contribute

Development of Dagger is open on GitHub, and we are grateful to the community for contributing bugfixes and improvements. Read below to learn how you can take part in improving Dagger.

Read our [contributing guide](docs/contribute/contribution.md) to learn about our development process, how to propose bugfixes and improvements, and how to build and test your changes to Dagger.

To help you get your feet wet and get you familiar with our contribution process, we have a list of [good first issues](https://github.com/odpf/dagger/labels/good%20first%20issue) that contain bugs which have a relatively limited scope. This is a great place to get started.

## Credits

This project exists thanks to all the [contributors](https://github.com/odpf/dagger/graphs/contributors).

## License
Dagger is [Apache 2.0](LICENSE) licensed.