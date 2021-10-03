# Dagger
![build workflow](https://github.com/odpf/dagger/actions/workflows/build.yml/badge.svg)
![package workflow](https://github.com/odpf/dagger/actions/workflows/package.yml/badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache)](LICENSE)
[![Version](https://img.shields.io/github/v/release/odpf/dagger?logo=semantic-release)](https://github.com/odpf/dagger/releases/latest)

Dagger or Data Aggregator is an easy-to-use, configuration over code, cloud-native framework built on top of Apache Flink for stateful processing of real-time streaming data. With Dagger, you don't need to write custom applications or manage resources to process data in real-time.
Instead, you can write SQLs to do the processing and analysis on streaming data.

<p align="center"><img src="./docs/static/img/overview.svg" /></p>

## Key Features
Discover why to use Dagger

* **Processing:** Dagger can transform, aggregate, join and enrich Protobuf data in real-time.
* **Scale:** Dagger scales in an instant, both vertically and horizontally for high performance streaming sink and zero data drops.
* **Extensibility:** Add your own sink to dagger with a clearly defined interface or choose from already provided ones.
* **Pluggability:** Add custom business logic in form of plugins \(UDFs, Transformers, Preprocessors and Post Processors\) independent of the core logic. 
* **Metrics:** Always know what’s going on with your deployment with built-in [monitoring](https://odpf.github.io/dagger/docs/reference/metrics) of throughput, response times, errors and more.

## What problems Dagger solves?
* Map reduce -> [SQL](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html)
* Enrichment -> [Post Processors](https://odpf.github.io/dagger/docs/advance/post_processor)
* Aggregation -> [SQL](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html), [UDFs](https://odpf.github.io/dagger/docs/guides/use_udf)
* Masking -> [Hash Transformer](https://odpf.github.io/dagger/docs/reference/transformers#HashTransformer)
* Deduplication -> [Deduplication Transformer](https://odpf.github.io/dagger/docs/reference/transformers#DeDuplicationTransformer)
* Realtime long window processing -> [Longbow](https://odpf.github.io/dagger/docs/advance/longbow)

To know more, follow the detailed [documentation](https://odpf.github.io/dagger/).

## Usage

Explore the following resources to get started with Dagger:

* [Guides](https://odpf.github.io/dagger/docs/guides/overview) provides guidance on [creating Dagger](https://odpf.github.io/dagger/docs/guides/create_dagger) with different sinks.
* [Concepts](https://odpf.github.io/dagger/docs/concepts/overview) describes all important Dagger concepts.
* [Advance](https://odpf.github.io/dagger/docs/advance/overview) contains details regarding advance features of Dagger.
* [Reference](https://odpf.github.io/dagger/docs/reference/overview) contains details about configurations, metrics and other aspects of Dagger.
* [Contribute](https://odpf.github.io/dagger/docs/contribute/contribution) contains resources for anyone who wants to contribute to Dagger.
* [Usecase](https://odpf.github.io/dagger/docs/usecase/overview) describes examples use cases which can be solved via Dagger.

## Running locally
Make sure you have Java8 and local kafka-2.4+ setup pre-installed on your local machine.
```sh
# Clone the repo
$ git clone https://github.com/odpf/dagger.git  

# Build the jar
$ ./gradlew clean build 

# Configure env variables
$ cat dagger-core/env/local.properties

# Run a Dagger
$ ./gradlew dagger-core:runFlink
```
**Note:** Sample configuration for running a basic dagger can be found [here](https://odpf.github.io/dagger/docs/guides/create_dagger#common-configurations). For detailed configurations, refer [here](https://odpf.github.io/dagger/docs/reference/configuration).

Find more detailed steps on local setup [here](https://odpf.github.io/dagger/docs/guides/create_dagger).

## Running on cluster
Refer [here](https://odpf.github.io/dagger/docs/guides/deployment) for details regarding Dagger deployment.

## Running tests 
```sh
# Running unit tests
$ ./gradlew clean test

# Run code quality checks
$ ./gradlew checkstyleMain checkstyleTest

# Cleaning the build
$ ./gradlew clean
```

## Contribute

Development of Dagger happens in the open on GitHub, and we are grateful to the community for contributing bug fixes and improvements. Read below to learn how you can take part in improving Dagger.

Read our [contributing guide](https://odpf.github.io/dagger/docs/contribute/contribution) to learn about our development process, how to propose bug fixes and improvements, and how to build and test your changes to Dagger.

To help you get your feet wet and get you familiar with our contribution process, we have a list of [good first issues](https://github.com/odpf/dagger/labels/good%20first%20issue) that contain bugs which have a relatively limited scope. This is a great place to get started.

## Credits
This project exists thanks to all the [contributors](https://github.com/odpf/dagger/graphs/contributors).

## License
Dagger is [Apache 2.0](LICENSE) licensed.
