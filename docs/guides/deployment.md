# Deployment

Dagger uses Flink for real-time distributed stream processing. It can run locally on your machine on single JVM process or on standalone mode (single machine but separate processes) for testing/debugging purposes.

Though for production environment you need to set up Flink on some supported resource provider like Kubernetes, YARN etc. which takes care of fault tolerance of processes and resource allocations.

To know more about Flink's cluster mode deployment follow [this](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/deployment/overview/).

## Cluster Deployment

Dagger currently supports `Flink-1.9`. So you need to set up a Flink-1.9 cluster in one of the supported resource manager or could vendors.

In the supported version of flink, the following resource managers/cloud vendors are supported.

- [Yarn](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/yarn_setup.html)
- [Mesos](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/mesos.html)
- [Docker](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/docker.html)
- [Kubernetes](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/kubernetes.html)
- [Map-Reduce](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/mapr_setup.html)
- [AWS](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/aws.html)
- [GCP](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/deployment/gce_setup.html)

If you are deploying Flink on Standalone or Yarn you need set up [zookeeper](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/jobmanager_high_availability.html) for high availability.

We prefer deploying Dagger on Kubernetes standalone environment with high availability set up with some modifications. Find a sample reference to the helm charts [here](https://github.com/docker-flink/examples/tree/master/helm/flink).
