# Deployment

Dagger uses Flink for distributed stream processing. It can run locally on your machine on a single JVM process or standalone mode (single machine but separate processes) for testing/debugging purposes.

For the production environment you can set up Flink on some supported resource provider like Kubernetes, YARN etc. which takes care of fault tolerance of processes and resource allocations.

To know more about Flink's cluster mode deployment follow [this](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/index.html).

## Build Jar Locally

- To run a dagger job in any of the supported environments you need to generate a fat jar with all the dependencies pre-loaded. The jar needs to be uploaded to Flink before running a dagger job.

```bash
# local clean build
./gradlew clean build

#creating a fat jar
./gradlew :dagger-core:fatJar

```

- After the jar is generated it should be available as `dagger-core/build/libs/dagger-core-version-all.jar`.
- Upload to cluster can be done using [REST api](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/ops/rest_api/#jars-upload) or [CLI](https://ci.apache.org/projects/flink/flink-docs-master/docs/deployment/cli/#submitting-a-job) or manually from the cluster's user interface.
- For optimized jar size in the cluster we also have another set-up where we divide the fat jar into two different jars called `-minimal.jar` and `-dependencies.jar`. As the minimal jar is the jar for dagger code base only without any dependencies whereas the dependencies jar contains all the external dependencies for running a dagger. Since the external dependencies do not change that much it's kind of static and can be updated to the cluster directly. And you will run a dagger with updating the minimal jar to the cluster.

```bash
# local minimal jar creation
./gradlew :dagger-core:minimalJar

#creating a dependency jar
./gradlew :dagger-core:dependenciesJar

```

## Cluster Deployment

Dagger currently supports `Flink-1.9`. So you need to set up a Flink-1.9 cluster in one of the supported resource managers or cloud vendors.

In the supported version of flink, the following resource managers/cloud vendors are supported.

- [Yarn](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/yarn_setup.html)
- [Mesos](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/mesos.html)
- [Docker](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/docker.html)
- [Kubernetes](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/kubernetes.html)
- [Map-Reduce](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/mapr_setup.html)
- [AWS](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/aws.html)
- [GCP](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/gce_setup.html)

If you are deploying Flink on Standalone or Yarn you need to set up [zookeeper](https://zookeeper.apache.org/) for [high availability](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/jobmanager_high_availability.html).

We prefer deploying Dagger on Kubernetes standalone environment with high availability set up with some modifications. Find a sample reference to the helm charts [here](https://github.com/docker-flink/examples/tree/master/helm/flink).
