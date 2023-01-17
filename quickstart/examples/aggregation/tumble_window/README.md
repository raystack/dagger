# Data Aggregation using a Tumble Window

## About this example
In this example, we will count the number of booking orders,(as Kafka records) in every 30 second interval. By the end of this example we will understand how to use Dagger to aggregate data over a specified time window.


## Before Trying This Example


1. **We must have Docker installed**. We can follow [this guide](https://docs.docker.com/get-docker/) on how to install and set up Docker in your local machine.
2. Clone Dagger repository into your local

   ```shell
   git clone https://github.com/odpf/dagger.git
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
   Hang on for a while as it installs all the required dependencies and starts all the required services. After a while we should see the output of the Dagger SQL query in the terminal, which will be the count of booking orders in every 30 second interval.
3. fire this command to gracefully close the docker compose:
   ```shell
   docker compose down 
   ```
   This will stop and remove all the containers.
   
Congratulations, we are now able to use Dagger for performing aggregation over a tumble window!