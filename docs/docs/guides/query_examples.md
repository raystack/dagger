# Example Queries

Dagger uses Apache Calcite as the querying framework. Find the documentation for the same [here](https://calcite.apache.org/docs/reference.html). Templates of some of the commonly used types of queries are listed down.

## Sample input schema

### Sample booking event schema

```protobuf
message SampleBookingInfo {
  string order_number = 1;
  string order_url = 2;
  string status = 3;
  google.protobuf.Timestamp event_timestamp = 4;
  string customer_id = 5;
  string driver_id = 6;
  string service_type = 7;
  string service_area_id = 8;
  repeated Item items = 9;
  map<string, string> metadata = 10;
}
```

```protobuf
message Item {
  string id = 1;
  int32 quantity = 2;
  string name = 3;
}
```

### Sample payment event schema

```protobuf
message SamplePaymentInfo {
  string order_id = 1;
  string customer_id = 2;
  google.protobuf.Timestamp event_timestamp = 3;
  int64 order_amount = 4;
}
```

## Influx Sink

- While using Dagger with InfluxDB sink, `tag_` should be appended to the beginning of those columns which you want as dimensions. Dimensions will help you slice the data in InfluxDB-Grafana. InfluxDB tags are essentially the columns on which data is indexed. Find more on influxDB tags [here](https://docs.influxdata.com/influxdb/v1.8/concepts/glossary/#tag).
- DO NOT use `tag_` for high cardinal data points such as customer_id, merchant_id etc unless you provide a filtering condition; this will create tag explosion & affect the InfluxDB.
- Ensure there is at least one value field present in the query(not starting with `tag_`).
- In case you want your dimensions without the prefix `tag_` you can use `label_` prefix. The name of the dimension will not have `tag_` or `label_` prefix.

### Example query

Here booking denotes the booking events stream with [sample booking schema](#sample-booking-event-schema).

```SQL
SELECT
 order_number,
 service_type as tag_service_id,
 status
from
  `booking`
```

## Kafka Sink

- `Tag_` prefix should not be used before the dimensions.
- Ensure that sink type is selected as Kafka.
- Dimensions & metrics from the SELECT section in the query should be mapped exactly to the field names in the output proto.
- Data types of the selected fields should exactly match to the output proto fields.
- Unlike Influx sink Dagger, high cardinality should not be an issue in Kafka sink.

### Example query

Here booking denotes the booking events stream with [sample booking schema](#sample-booking-event-schema).

```SQL
SELECT
 order_number,
 service_type,
 status
from
  `booking`
```

## Aggregation - Tumble window

- Use this for aggregating data points using a TUMBLE window function (data aggregated for all points in the window at the end of each cycle).
- Use the windowing function for Tumble window using the keyword TUMBLE_END(datetime, INTERVAL 'duration' unit) & TUMBLE (datetime, INTERVAL 'duration' unit ) in SELECT & GROUP BY section respectively (duration is a number & unit can be SECOND/MINUTE/HOUR).

### Example query

Here booking denotes the booking events stream with [sample booking schema](#sample-booking-event-schema).

```SQL
SELECT
  count(1) as booking_count,
  TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp
from
  `booking`
GROUP BY
  TUMBLE (rowtime, INTERVAL '60' SECOND)
```

## Aggregation - Hop window

- Use this for aggregating data points using a HOP window function (data aggregated at every slide internal for all points in the window interval).
- Use the windowing function for Hop window using the keyword HOP_END(datetime, SLIDE_INTERVAL 'duration' unit, WINDOW_INTERVAL 'duration' unit) & HOP (datetime, SLIDE_INTERVAL 'duration' unit, WINDOW_INTERVAL 'duration' unit) in SELECT & GROUP BY section respectively (both slide interval & window interval are numbers & units can be SECOND/MINUTE/HOUR).

### Example query

Here booking denotes the booking events stream with [sample booking schema](#sample-booking-event-schema).

```SQL
SELECT
  service_area_id AS tag_service_area_id,
  count(1) AS number_of_bookings,
  HOP_END(
    rowtime,
    INTERVAL '60' SECOND,
    INTERVAL '1' HOUR
  ) AS window_timestamp
FROM
  `booking`
GROUP BY
  HOP(
    rowtime,
    INTERVAL '60' SECOND,
    INTERVAL '1' HOUR
  ),
  service_area_id
```

## Subquery/Inner Query

- You can use as many inner queries as required.
- In case you want to use window aggregations both in inner and outer query, use [TUMBLE_ROWTIME](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html#time-attributes) to get the output rowtimes from the inner window.

### Example query

Here booking denotes the booking events stream with [sample booking schema](#sample-booking-event-schema).

```SQL
SELECT
  booking_count,
  cancelled_order,
  window_timestamp
FROM
  (
    SELECT
      count(1) as booking_count,
      cast(
        (
          Sum(
            Case
              When status in (
                'CUSTOMER_CANCELLED'
              ) Then 1
              Else 0
            End
          )
        ) as float
      ) as cancelled_order,
      TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp
    from
      `booking`
    GROUP BY
      TUMBLE (rowtime, INTERVAL '60' SECOND)
  )
```

## Feast Row Transformation

This sample query is for transforming data to Feature rows for [Feast](https://github.com/feast-dev/feast/tree/master/docs#introduction) using [Features UDF](../reference/udfs.md#Features).

### Example query

Here booking denotes the booking events stream with [sample booking schema](#sample-booking-event-schema).

```SQL
SELECT
  'MINUTE' AS granularity,
  customer_id AS entityKey,
  'customers' AS entityName,
  Features(
    LOWER(service_type),
    customer_id
  ) AS features,
  TUMBLE_END(rowtime, INTERVAL '1' MINUTE) AS eventTimestamp
FROM
  `booking`
GROUP BY
  TUMBLE (rowtime, INTERVAL '1' MINUTE),
  customer_id
```

## Inner Join

- In order to keep state size in check, we recommend using time interval joins.
- Time interval should be in the format table2.rowtime BETWEEN table1.rowtime - INTERVAL 'first duration' unit AND table1.rowtime + INTERVAL 'second duration' unit (both durations are numbers & units can be SECOND/MINUTE/HOUR)

### Example query(no with)

Here booking denotes the booking events stream with [sample booking schema](#sample-booking-event-schema) and payment denotes payment stream with [sample payment schema](#sample-payment-event-schema).

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

### Example query(using with)

Here booking denotes the booking events stream with [sample booking schema](#sample-booking-event-schema) and payment denotes payment stream with [sample payment schema](#sample-payment-event-schema).

```SQL
WITH booking_info AS (
  SELECT
    service_type,
    customer_id,
    order_number,
    rowtime
  FROM
    `booking`
),
payment_info AS (
  SELECT
    order_amount
    order_id,
    rowtime
  FROM
    `payment`
)
SELECT
  booking_info.service_type as tag_service_type,
  count(booking_info.order_number) as number_of_orders,
  sum(payment_info.order_amount) as total_amount,
  TUMBLE_END(booking_info.rowtime, INTERVAL '60' MINUTE) AS window_timestamp
from
  `booking_info`
  join `payment_info` ON booking_info.order_number = payment_info.order_id
  AND payment_info.rowtime BETWEEN booking_info.rowtime
  AND booking_info.rowtime + INTERVAL '5' MINUTE
GROUP BY
  TUMBLE (booking_info.rowtime, INTERVAL '60' MINUTE),
  booking_info.service_type
```

## Union

In union, the fields selected from 2 streams can be combined to create a new stream. Ensure that the same field names are present on both the stream select outputs.

### Example query

Here booking denotes the booking events stream with [sample booking schema](#sample-booking-event-schema) and payment denotes payment stream with [sample payment schema](#sample-payment-event-schema).

```SQL
SELECT
  customer_id,
  service_type,
  order_number,
  service_area_id,
  event_timestamp
FROM `booking`
UNION ALL
  (SELECT
    customer_id,
    '' AS service_type,
    order_id AS order_number,
    '' AS service_area_id,
    event_timestamp
  FROM `payment`)
```

## Unnest

- Use Unnest to flatten arrays present in the schema
- In the FROM section, use the Unnest function after the table name in the following syntax FROM booking, UNNEST(booking.items) AS items (id, quantity, name)
- All fields from the array object should be added while unnesting

In the above example, to retrieve the unnested fields, use items.name, items.quantity etc.

### Example query(list)

Here booking denotes the booking events stream with [sample booking schema](#sample-booking-event-schema).

```SQL
SELECT
  tag_itemname,
  num_orders,
  window_timestamp
FROM
  (
    SELECT
      items.name as tag_itemname,
      count(1) as num_orders,
      TUMBLE_END(rowtime, INTERVAL '5' MINUTE) AS window_timestamp
    FROM
      booking,
      UNNEST(booking.items) AS items (id, quantity, name)
    GROUP BY
      TUMBLE (rowtime, INTERVAL '5' MINUTE),
      items.name
  )
WHERE
  num_orders > 100
```

### Example query(map)

In Dagger, we deserialize maps also as list so unnest works in the following way.

Here booking denotes the booking events stream with [sample booking schema](#sample-booking-event-schema).

```SQL
SELECT
  index_map.`key` AS data_in_key,
  index_map.`value` AS data_in_value,
FROM booking, UNNEST(booking.metadata) AS index_map (`key`, `value`)
```
