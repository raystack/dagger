# Development Guide

The following segment is a foundation for developing things on top of Dagger.

The main logical blocks of Dagger are:

- `Streams`: All Kafka source related information for Data consumption.
- `SQL`: SQL Query to process input stream data.
- `Processors`: Plugins to define custom Operators and to interact with external data sources.
- `Sink`: Sinking data after processing is done.

## Run a Dagger

### Development environment

The following environment is required for Dagger development

- Java SE Development Kit 8.

### Services

The following components/services are required to run Dagger:

- Kafka &gt; 2.4 to consume messages from.
- Corresponding sink service to sink data to.
- Flink Cluster \(Optional\) only needed if you want to run in cluster mode. For standalone mode, it's not required.

### Local Creation

Follow [dagger creation guide](../guides/create_dagger.md) for creating/configuring a Dagger in the local environment and to know more about the basic stages of Dagger.

## Style Guide

### Java

We conform to the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). Maven can helpfully take care of that for you before you commit.

## Code Modules

Dagger follows a multi-project build structure with multiple sub-projects. This allows us to maintain the code base logically segregated and remove the duplications. Describing briefly each submodule.

- **`dagger-core`**: The core module in Dagger. Accommodates All the core functionalities like SerDE, SQL execution, Processors and many more. Also efficiently interacts with other subprojects.
- **`dagger-common`**: This module contains all the code/contracts that are shared between other submodules. This allows reducing code duplicate and an efficient sharing of code between other submodules. For example, MetricsManagers are part of this submodule since metrics need to be recorded both in `dagger-common` and `dagger-functions`.
- **`dagger-functions`**: Submodule that defines all the plugin components in Dagger like [UDFs](https://github.com/odpf/dagger/tree/main/dagger-functions/src/main/java/io/odpf/dagger/functions/udfs) and [Transformers](https://github.com/odpf/dagger/tree/main/dagger-functions/src/main/java/io/odpf/dagger/functions/transformers).
- **`dagger-tests`**: Integration test framework to test some central end to end flows in Dagger.

## Dependencies Configuration

- Dagger generates three different types of jars. One traditional fat/shadow.jar with all the dagger specific code + the external dependencies, that is sufficient for running a dagger. Since this jar becomes heavyweight (~200MB) and takes few seconds to get uploaded to the cluster, Dagger provides another setup for jar creation where we segregate the dependencies from the Dagger code base.
- The external dependencies in Dagger are less frequently updated. So we treat them as static and update on a major release.
- The more frequently changing Dagger codebase reside in the minimal jar whose size is in KBs and gets uploaded to the cluster in a flash. The deployment section talks more about jar creation and how to deploy them to the Flink cluster.
- The jar separation is a part of Gradle configuration. We define two new sets of configurations in all of the Dagger submodules.
  - `minimalJar`: add all dependencies to be included in the Dagger cluster user jar.
  - `dependenciesJar`: add all dependencies to be included in the flink image.

## Integration Tests

Dagger interacts with external sources such as ElasticSearch, Postgres and HTTP endpoints to enrich the streams using post-processors. We have added Integration Tests to expose and resolve blunders in the interaction between integrated units at the time of development itself.

Integration Tests are part of the current CI set-up of Dagger and they must pass before releasing a version of Dagger. These will cover all the flows of postprocessors with different external sources and ensure a bug-free dagger.

### Dev Setup for Local Integration Tests

#### Setup Elasticsearch

```

# install elastic search

brew update
brew install elasticsearch
brew services start elasticsearch

```

#### Setup Postgres

```bash
# install postgres in local environment
brew update
brew install postgresql
#configure postgres for integration tests
psql postgres
>> create user root with password root
>> create ROLE root WITH LOGIN PASSWORD 'root' CREATEDB;
# Login Via root now and create test database
>> psql postgres -U root
>> postgres=>create database test_db;
```

#### Run Integration tests

```bash
./gradlew clean IntegrationTest
```
