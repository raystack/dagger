# Joining two Kafka topics using Inner join

## About this example

In this example, we will use the Inner joins in Dagger to join the data streams from two different Kafka topics and count the number of booking logs in every 30 second interval from both the sources combined for each service type. By the end of this example we will understand how to use inner joins to combine 2 or more Kafka streams.

## Before Trying This Example

1. **We must have Docker installed**. We can follow [this guide](https://docs.docker.com/get-docker/) on how to install and set up Docker in your local machine.
2. Clone Dagger repository into your local

   ```shell
   git clone https://github.com/raystack/dagger.git
   ```

## Steps

Following are the steps for setting up dagger in docker compose -

1. cd into the aggregation directory:
   ```shell
   cd dagger/quickstart/examples/aggregation/tumble_window
   ```
2. fire this command to spin up the docker compose:
   ```shell
   docker compose up
   ```
   Hang on for a while as it installs all the required dependencies and starts all the required services. After a while we should see the output of the Dagger SQL query in the terminal, which will be the number of booking logs in every 30 second interval from both the Kafka sources combined, for each service type.
3. fire this command to gracefully close the docker compose:
   ```shell
   docker compose down
   ```
   This will stop and remove all the containers.

Congratulations, we are now able to use Dagger to combine 2 or more Kafka streams.!
