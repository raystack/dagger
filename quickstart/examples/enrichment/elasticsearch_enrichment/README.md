# Stream enrichment using ElasticSearch source

## About this example

In this example, we will use Dagger Post-processors to enrich the payment transaction logs (from Kafka source), in the input stream with user profile information from an external source i.e. Elasticsearch, to get the user profile information in each record. At the end of this example, we will be able to use Dagger to enrich our data stream from Kafka with the data on any remote ElasticSearch server.

## Before Trying This Example

1. **You must have Docker installed**. We can follow [this guide](https://docs.docker.com/get-docker/) on how to install and set up Docker in your local machine.
2. Clone Dagger repository into your local

   ```shell
   git clone https://github.com/raystack/dagger.git
   ```

## Steps

Following are the steps for setting up dagger in docker compose -

1. cd into the aggregation directory:
   ```shell
   cd dagger/quickstart/examples/enrichment/elasticsearch_enrichment
   ```
2. fire this command to spin up the docker compose:
   ```shell
   docker compose up
   ```
   Hang on for a while as it installs all the required dependencies and starts all the required services. After a while we should see the output of the Dagger SQL query in the terminal, which will be the enriched booking log with the customer profile information.
3. fire this command to gracefully close the docker compose:
   ```shell
   docker compose down
   ```
   This will stop all services and remove all the containers.

Congratulations, we are now able to use Dagger to enrich our data stream from Kafka with the data on any remote ElasticSearch server.
