## Explore Custom UDFs

Some of the use-cases can not be solved using Flink SQL & the Apache Calcite functions. In such a scenario, Dagger can be extended to meet the requirements using User Defined Functions (UDFs). We use the Scalar UDF Distance in our example. Scalar UDFs map zero or more values to a new value. These functions are invoked for each data in the stream.


#### Distance UDF
* Contract:
    * **Double** `Distance(Double latitude1, Double longitude1, Double latitude2, Double longitude2)`
* Functionality:
    * Calculates the distance between two points in km with given latitude and longitude.
* Example:
```
SELECT
  Distance(
    data1_location.latitude,
    data1_location.longitude,
    data2_location.latitude,
    data2_location.longitude
  ) AS distance_data,
  TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp
FROM
  data_stream
where
  status = 'CUSTOMER_CANCELLED'
GROUP BY
  TUMBLE (rowtime, INTERVAL '60' SECOND),
  data1_location.latitude,
  data1_location.longitude,
  data2_location.latitude,
  data2_location.longitude
```

### Prerequisites

1. **You must have docker installed**

Following are the steps for setting up dagger in docker compose -
1. Clone Dagger repository into your local

   ```shell
   git clone https://github.com/odpf/dagger.git
   ```
2. cd into the udfs directory:
   ```shell
   cd dagger/quickstart/examples/udfs
   ```
3. fire this command to spin up the docker compose:
   ```shell
   docker compose up 
   ```
This will spin up docker containers for the kafka, zookeeper, stencil, kafka-producer and the dagger.
4. fire this command to gracefully close the docker compose:
   ```shell
   docker compose down 
   ```
   