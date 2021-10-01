# Darts

In data streaming pipelines, in certain cases, not entire data is present in the event itself. One scenario for such cases can be where some particular information is present in form of static data that you need in runtime. DARTS(Dagger Refer-Table Service) allows you to join streaming data from a reference data store. It supports reference data store in the form of a list or <key, value> map. It enables the refer-table with the help of [UDFs](../guides/use_udf.md) which can be used in the SQL query. Currently, we only support GCS as a reference data source.

# Types of DARTS

We currently support only [GCS](https://cloud.google.com/storage) as an external static data source. In order to utilize this data directly into Dagger SQL queries, we have enabled two different types of functions.

- [DartGet](DARTS.md#dartget)
- [DartContains](DARTS.md#dartcontains)

## DartGet

This UDF can be used in cases where we want to fetch static information from a <key, value> mapping. In this case, the key can be a field from input Kafka topic and corresponding value can be fetched from remote store(GCS). You can read more about this UDF [here](../reference/udfs.md#dartget).

### Example

Let’s assume we need to find out the number of bookings getting completed in a particular District per minute. The input schema only has information regarding service_area_id but not the District. The mapping of service_area_id to District is present in a static key-value map. We can utilize DartGet in order to get this information in our query.

![](/img/dart-get.png)

Sample input schema for booking

```protobuf
message SampleBookingInfo {
  string order_number = 1;
  string order_url = 2;
  Status.Enum status = 3;
  google.protobuf.Timestamp event_timestamp = 4;
  string customer_id = 5;
  string service_area_id = 6;
}
```

Sample static data for serviceAreaId-to-district mapping

```JSON
{
  "1": "district1",
  "2": "district2",
  "3": "district3",
  "4": "district4"
}
```

Sample Query

```SQL
# here booking denotes the booking events stream with the sample input schema
SELECT
  COUNT(DISTINCT order_number) as completed_bookings,
  DartGet('serviceAreaId-to-district/data.json', service_area_id, 24) AS district,
  TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp
FROM
  booking
WHERE
  status = 'COMPLETED'
GROUP BY
  DartGet('serviceAreaId-to-district/data.json', service_area_id, 24),
  TUMBLE (rowtime, INTERVAL '60' SECOND)
```

## DartContains

This UDF can be used in cases where we want to verify the presence of a key in a static list present remotely in GCS. You can read more about this UDF [here](../reference/udfs.md#dartcontains).

### Example

Let’s assume we need to count the number of completed bookings per minute but excluding a few blacklisted customers. This static list of blacklisted customers is present remotely. We can utilize DartContains UDF here.

![](/img/dart-contains.png)

Sample input schema for booking

```protobuf
message SampleBookingInfo {
  string order_number = 1;
  string order_url = 2;
  Status.Enum status = 3;
  google.protobuf.Timestamp event_timestamp = 4;
  string customer_id = 5;
  string service_area_id = 6;
}
```

Sample static data for blacklisted-customers

```JSON
{
  "data": ["1", "2", "3", "4"]
}
```

Sample Query

```SQL
# here booking denotes the booking events stream with the sample input schema
SELECT
  COUNT(DISTINCT order_number) as completed_bookings,
  TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp
FROM
  booking
WHERE
  status = 'COMPLETED' AND
  NOT DartContains('blacklisted-customers/data.json', customer_id, 24)
GROUP BY
  TUMBLE (rowtime, INTERVAL '60' SECOND)
```

**Note:** To use DartContains you need to provide the data in the form of a JSON array with Key as `data` always.

## Configurations

Most of DARTS configurations are via UDF contract, for other glabal configs refer [here](../reference/configuration.md#darts).

## Properties common to both DARTS UDFs

### Persistence layer

We store all data references in GCS. We recommend using GCS where the data reference is not too big and updates are very few. GCS also offers a cost-effective solution.
We currently support storing data in a specific bucket, with a custom folder for every dart. The data is always kept in a file and you have to pass the relative path `<custom-folder>/filename.json`. There should not be many reads as every time we read the list from GCS we read the whole selected list and cache it in Dagger.

### Caching mechanism

Dart fetches the data from GCS after configurable refresh period or when entire data is missing from the cache or is empty. After Dart fetches the data, it stores it in the application state.

### Caching refresh rate

We have defined the refresh rate in hours. Users can set the refresh rate from the UDF contract. We set the default value as one hour in case Dart users don't bother about the refresh rate.

### Updates in data

There is a refresh rate as part of the UDF. This defines when the data will be reloaded into memory from the data store. So after one cycle from manual data update, the changes will reflect into the Dagger.
