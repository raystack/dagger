## Aggregation - Tumble window

- Use this for aggregating data points using a TUMBLE window function (data aggregated for all points in the window at the end of each cycle).
- Use the windowing function for Tumble window using the keyword TUMBLE_END(datetime, INTERVAL 'duration' unit) & TUMBLE (datetime, INTERVAL 'duration' unit ) in SELECT & GROUP BY section respectively (duration is a number & unit can be SECOND/MINUTE/HOUR).

### Example query

Here booking denotes the booking events stream with sample booking schema.

```SQL
SELECT
  count(1) as booking_count,
  TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp
from
  `data_stream_0`
GROUP BY
  TUMBLE (rowtime, INTERVAL '60' SECOND)
```

## Docker Compose Setup

### Prerequisites

1. **You must have docker installed**

Following are the steps for setting up dagger in docker compose -
1. Clone Dagger repository into your local

   ```shell
   git clone https://github.com/odpf/dagger.git
   ```
2. cd into the aggregation directory:
   ```shell
   cd dagger/quickstart/examples/aggregation 
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
   