## Inner Join

- The INNER JOIN creates a new result table by combining column values of two tables (table1 and table2) based upon the join-predicate. The query compares each row of table1 with each row of table2 to find all pairs of rows which satisfy the join-predicate.
- In order to keep state size in check, we recommend using time interval joins.
- Time interval should be in the format table2.rowtime BETWEEN table1.rowtime - INTERVAL 'first duration' unit AND table1.rowtime + INTERVAL 'second duration' unit (both durations are numbers & units can be SECOND/MINUTE/HOUR)

### Example query

Here booking denotes the booking events stream with sample booking schema and payment denotes payment stream with sample payment schema.

```SQL
SELECT
  booking.service_type as tag_service_type,
  count(order_number) as number_of_orders,
  sum(order_amount) as total_amount,
  TUMBLE_END(booking.rowtime, INTERVAL '60' MINUTE) AS window_timestamp
from
  `booking`
  join `payment` ON booking.order_number = payment.order_id
  AND payment.rowtime BETWEEN booking.rowtime
  AND booking.rowtime + INTERVAL '5' MINUTE
GROUP BY
  TUMBLE (booking.rowtime, INTERVAL '60' MINUTE),
  booking.service_type
```

## Docker Compose Setup

### Prerequisites

1. **You must have docker installed**

Following are the steps for setting up dagger in docker compose -
1. Clone Dagger repository into your local

   ```shell
   git clone https://github.com/odpf/dagger.git
   ```
2. cd into the joins directory:
   ```shell
   cd dagger/quickstart/examples/joins 
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
   