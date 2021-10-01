# Udfs

This page contains references for all the custom udfs available on Dagger.

## List of Udfs

- [Scalar Functions](udfs.md#scalar-functions)
  - [ArrayAggregate](udfs.md#ArrayAggregate)
  - [ArrayOperate](udfs.md#ArrayOperate)
  - [CondEq](udfs.md#CondEq)
  - [DartContains](udfs.md#DartContains)
  - [DartGet](udfs.md#DartGet)
  - [Distance](udfs.md#Distance)
  - [ElementAt](udfs.md#ElementAt)
  - [EndOfMonth](udfs.md#EndOfMonth)
  - [EndOfWeek](udfs.md#EndOfWeek)
  - [ExponentialMovingAverage](udfs.md#ExponentialMovingAverage)
  - [Filters](udfs.md#Filters)
  - [FormatTimeInZone](udfs.md#FormatTimeInZone)
  - [GeoHash](udfs.md#GeoHash)
  - [LinearTrend](udfs.md#LinearTrend)
  - [ListContains](udfs.md#ListContains)
  - [MapGet](udfs.md#MapGet)
  - [S2AreaInKm2](udfs.md#S2AreaInKm2)
  - [S2Id](udfs.md#S2Id)
  - [SelectFields](udfs.md#SelectFields)
  - [SingleFeatureWithType](udfs.md#SingleFeatureWithType)
  - [Split](udfs.md#Split)
  - [StartOfMonth](udfs.md#StartOfMonth)
  - [StartOfWeek](udfs.md#StartOfWeek)
  - [TimeInDate](udfs.md#TimeInDate)
  - [TimestampFromUnix](udfs.md#TimestampFromUnix)
- [Aggregate Functions](udfs.md#aggregate-functions)
  - [CollectArray](udfs.md#CollectArray)
  - [DistinctCount](udfs.md#DistinctCount)
  - [Features](udfs.md#Features)
  - [FeaturesWithType](udfs.md#FeaturesWithType)
  - [PercentileAggregator](udfs.md#PercentileAggregator)
- [Table Functions](udfs.md#table-functions)
  - [HistogramBucket](udfs.md#HistogramBucket)
  - [OutlierMad](udfs.md#OutlierMad)
  
### Scalar Functions

#### ArrayAggregate
* Contract: 
  * **Object** `ArrayAggregate(Object[] arrayElements, String operationType, String inputDataType)`.
* Functionality:
  * This is one of the UDFs related to **LongbowPlus** but also can be used alone. Given an Object Array, this UDF performs basic Mathematical functions on the Array. You need to give the name of the function you want to use as a string and the Data type of the element inside the input array. Find details on LongbowPlus [here](../advance/longbow_plus.md).
  * Currently, we only support limited operations on limited data types. The supported Data types and functions are listed below.
* Supported Data types and Functions:
  * Data type:
    * Integer
    * Float
    * Double
    * Long
    * Other(all other data types except the above)
  * Functions:
    * Basic Mathematical aggregation functions like ‘average’, ‘sum’, ‘min’, ’max’, ’count’, ’findFirst’ etc.
    * Do some basic complex aggregations like distinct counts. You need to give an argument like ‘distinct.count’ as operationType.Prefixes have to be added before actual functions with a ‘.’ whenever required. The first operation here needs to output another Array. So for now, ‘sort’ and ‘distinct’ are only supported as the first element in complex types.
    * In the case of Float, the output will be in double. 
    * For ‘Other’ data types some basic operations like ‘count’, ‘findFirst’ are supported. You can also do something like ‘distinct.count’/’sort.findFirst’ here, but no other mathematical functions are supported here.
    * Listing here all the available functions for corresponding data types
      * Mathematical Data Types:
        'average', 'min', 'max', 'sum', 'count', 'findFirst', 'findAny'
      * Prefix:
        'sort', 'distinct'
      * Other Data Types:
        'count', 'findFirst', 'findAny'
* Example:
  * When used a single aggregation
    ```
    SELECT 
      ArrayAggregate(
        SelectFields(proto_data, input_class_name, 'shopping_price'),
        'sum',
        'float') AS total_shoping_price_value
    FROM 
      data_stream
    ```
  * When used a basic double aggregation
      ```
      SELECT 
        ArrayAggregate(
          SelectFields(proto_data, input_class_name, 'shopping_price'),
          'distinct.sum',
          'float') AS average_value
      FROM 
        data_stream
      ```

#### ArrayOperate
* Contract: 
  * **Object[]** `ArrayOperate(Object[] arrayElements, String operationType, String inputDataType)`
* Functionality:
  * This is one of the UDFs related to **LongbowPlus** but also can be used alone. Given an Object Array, this UDF performs basic Mathematical functions on the Array. You need to give the name of the function you want to use as a string and the Data type of the element inside the input array. Find details on LongbowPlus [here](../advance/longbow_plus.md).
  * Currently, we only support limited operations on limited data types. The supported Data types and functions are listed below.
* Supported Data types and Functions:
  * Data type:
    * Integer
    * Float
    * Double
    * Long
    * Other(all other data types except the above. For String you need to process it as Other)
  * Functions:
    * Distinct
    * Sorted
* Example:

```
SELECT 
  ArrayOperate(
    SelectFields(proto_data, input_class_name, 'shopping_price'),
    'distinct', 
    'float') AS distinct_shoping_price_values
FROM 
  data_stream
```

#### ByteToString
* Contract:
  * **String** `ByteToString(ByteString byteField)`
* Functionality:
  * Given a ByteString, this UDF converts to String
* Example:

```
Select
    ByteToString(payload) as payload_string
from
    data_streams
```

#### CondEq
* Contract: 
  * `Predicate<DynamicMessage>** CondEq(String fieldName, Object comparison)`
* Functionality:
  * This is one of the UDFs related to **LongbowPlus** and has to be used with **SelectFields** and **Filters** UDFs. Find details on LongbowPlus [here](../advance/longbow_plus.md).
  * Can specify an equality condition with a fieldName and a value.
* Example:

```
SELECT 
  SelectFields(
    proto_data,
    input_class_name,
    'order_number'
  ) AS favourite_service_provider_guids
FROM 
  data_stream
WHERE 
  cardinality(
    SelectFields(
      Filters(
        proto_data, 
        input_class_name, 
        CondEq('status', 'CUSTOMER_CANCELLED')
      ),
      'order_number' 
    )
  ) > 0
```

#### DartContains
* Contract: 
  * **Boolean** `DartContains(String collectionName, String value, int cacheTTLin_hours)` OR
  * **Boolean** `DartContains(String collectionName, String value, String regex, int cacheTTLin_hours)`
* Functionality:
  * Check if a data point in the message is present in the GCS bucket
  * Regex can you used to create the pattern using values from GCS to match against the field value.
* Example
  * Without regex
  ```
  SELECT 
    data1,
    data2,
    TUMBLE_END(rowtime, INTERVAL '1' HOUR) AS event_timestamp
  FROM 
    data_stream
  WHERE 
    DartContains('test_collection', data1, 24)
  GROUP BY
    TUMBLE (rowtime, INTERVAL '1' HOUR)
  ```
  * With regex 
  ```
  SELECT 
    data1,
    data2,
    TUMBLE_END(rowtime, INTERVAL '1' HOUR) AS event_timestamp
  FROM 
    data_stream
  WHERE 
    DartContains('test_collection', data1, '^%s.*' 24)
  GROUP BY
    TUMBLE (rowtime, INTERVAL '1' HOUR)
  ```

#### DartGet
* Contract:
  * **String** `DartGet(String collectionName, key, int cacheTTLin_hour)`
* Functionality: 
  * Corresponding value in a GCS bucket given a key from data point
* Example:
```
SELECT
  DartGet('test_collection', data1, 24) AS tag_data1,
  TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp,
FROM
  data_stream
GROUP BY
  TUMBLE (rowtime, INTERVAL '60' SECOND)
```

#### Distance
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

#### ElementAt
* Contract:
  * **String** `ElementAt(Row[] array, String pathOfArray, int index, String path, String tableName)`
  * **String** `ElementAt(Row[] array, String pathOfArray, int index, String path)`
  * **Object** `ElementAt(Object[] array, int index)`
  * **Object** `ElementAt(ArrayList\<Object\> arrayList, int index)`
* Functionality: 
  * For the given table name from the streams (In the case of multi-streams/JOINS), find out the element at a given index and a given path in an array of complex Data Types.
  * Finds out the element at a given index and a given path in an array of complex Data Types. Here table name is not provided, In that case, it will always apply the function on the table from the first stream in the configuration.
  * Finds out the element at a given index in case of an object Array.
  * Finds out the element at a given index in case of an object Arraylist.
* Examples:
  * Example 1:
  ```
  SELECT
    CAST(
      ElementAt(test_data, 'data', 0, 'test_location.latitude', 'data_stream_2') AS double
    ) AS lat,
    CAST(
      ElementAt(test_data, 'data', 0, 'test_location.longitude', 'data_stream_2') AS double
    ) AS long,
    TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp
  FROM
    data_stream_1
  GROUP BY
    TUMBLE (rowtime, INTERVAL '60' SECOND),
    ElementAt(test_data, 'data', 0, 'test_location.latitude', 'data_stream_2'),
    ElementAt(test_data, 'data', 0, 'test_location.longitude', 'data_stream_2')
  ```
  * Example 2:
  ```
  SELECT
    CAST(
      ElementAt(test_data, 'data', 0, 'test_location.latitude') AS double
    ) AS lat,
    CAST(
      ElementAt(test_data, 'data', 0, 'test_location.longitude') AS double
    ) AS long,
    TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp
  FROM
    data_stream
  GROUP BY
    TUMBLE (rowtime, INTERVAL '60' SECOND),
    ElementAt(test_data, 'data', 0, 'test_location.latitude'),
    ElementAt(test_data, 'data', 0, 'test_location.longitude')
  ```
  * Example 3:
  ```
  WITH tmpTable AS (
    SELECT
    SelectFields(
      proto_data,
      input_class_name,
      'cancel_reason_id'
    ) AS test_data_array,
    longbow_read_key AS restaurant_id
    FROM
    data_stream
  )
  SELECT
    restaurant_id,
    ElementAt(test_data_array, -1) AS last_test_data_array,
    ElementAt(test_data_array, -2) AS second_last_test_data_array,
    ElementAt(test_data_array, -3) AS third_last_test_data_array
  FROM
    tmpTable
  ```
  
#### EndOfMonth
* Contract: 
  * **Long** `EndOfMonth(long seconds, String timeZone)`
* Functionality:
  * Calculates the seconds in Unix time for the end of a month of a given timestamp second and timezone.
* Example:
```
SELECT 
  EndOfMonth(
    event_timestamp.seconds, 
    'America/New_York') AS T
FROM data_stream
```

#### EndOfWeek
* Contract: 
  * **Long** `EndOfWeek(long seconds, String timeZone)`
* Functionality:
  * Calculates the seconds in Unix time for the end of a week of a given timestamp second and timezone.
* Example:
```
SELECT 
  EndOfWeek(
    event_timestamp.seconds,
    'America/New_York') AS T
FROM data_stream
```

#### ExponentialMovingAverage
* Contract: 
  * **Double** `ExponentialMovingAverage(ArrayList\<Timestamp\> timestampsArray, ArrayList\<Double\> valuesArray, Timestamp hopStartTime, Double window, Double alpha)`
* Functionality:
  * Calculates exponential moving average (at per minute frequency) using a list of non-null values. Parameters are window (in minutes) and alpha. The hopStartTime and the corresponding list of timestamps denote the sequence of the non-null values in the window.
* Example:
```
SELECT
  CAST(
    ExponentialMovingAverage(
      timestamps_array,
      values_array,
      hop_start_time,
      CAST(15 AS DOUBLE),
      CAST(0.2 AS DOUBLE)
    ) AS BIGINT
  ) AS unique_data,
  event_timestamp
FROM
  data_stream
```

#### Filters

* Contract: 
  * `List<DynamicMessage>** Filters(ByteString[] inputProtoBytes, String protoClassName, Predicate<DynamicMessage>... predicates)`
* Functionality:
  * This is one of the UDFs related to **LongbowPlus** and has to be used with **SelectFields** and **CondEq** UDFs. Find details on LongbowPlus [here](../advance/longbow_plus.md).
  * Takes ByteString[] as the data and zero or more Predicates (we have only CondEq as a predicate that is defined for now). Applies the predicated conditions on the proto ByteString list field that is selected from the query and returns filtered Data.
* Example:
```
SELECT 
  SelectFields(
    proto_data,
    input_class_name,
    'order_number'
  ) AS favourite_service_provider_guids
FROM 
  data_stream
WHERE 
  cardinality(
    SelectFields(
      Filters(
        proto_data, 
        input_class_name, 
        CondEq('status', 'CUSTOMER_CANCELLED')
      ),
      'order_number' 
    )
  ) > 0
```

#### FormatTimeInZone
* Contract: 
  * **String** `FormatTimeInZone(Timestamp timestamp, String timeZone, String dateFormat)`
* Functionality:
  * Gets formatted time from timestamp in given timezone.
* Example:
```
SELECT 
  FormatTimeInZone(
    event_timestamp,
    'America/New_York',
    'yyyy-MM-dd'
  ) AS T
FROM data_stream
```

#### GeoHash
* Contract: 
  * **GeoHash**`(Double latitude, Double longitude, int level)`
* Functionality:
  * Returns a geohash for a given level and lat-long for the given WGS84 point.
* Example:
```
SELECT
  data1_location.longitude AS long,
  data1_location.latitude AS lat,
  GeoHash(
    data1_location.longitude,
    data1_location.latitude,
    6
  ) AS geohashPickup,
  TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp
FROM
  data_stream
GROUP BY
  TUMBLE (rowtime, INTERVAL '60' SECOND),
  data1_location.longitude,
  data1_location.latitude
```

#### LinearTrend
* Contract: 
  * **Double** `LinearTrend(ArrayList<Timestamp> timestampsArray, ArrayList<Double> demandList, Timestamp hopStartTime, Integer windowLengthInMinutes)`
* Functionality:
  * Returns the gradient of the best fit line of the list of non-null demand values given the defined time window. hopStartTime and timestampsArray denote the sequence of non-null demand values in the window.
  * Find more details on Linear Trend Algorithm [here](https://en.wikipedia.org/wiki/Linear_trend_estimation).
* Example:
```
SELECT
  CAST(
    LinearTrend(
      timestamps_array,
      values_array,
      hop_start_time,
      15
    ) AS BIGINT
  ) AS unique_data,
  event_timestamp
FROM
  data_stream
```

#### ListContains
* Contract: 
  * **Boolean** `ListContains(String[] inputList, String item)`
* Functionality:
  * Checks if a list contains a given item.
* Example:
```
SELECT 
  data1,
  data2
FROM 
  data_stream
WHERE 
  ListContains(tests, 'test1')
```

#### MapGet
* Contract: 
  * **Object** `MapGet(Row[] inputMap, Object key)`
* Functionality:
  * Returns value for a corresponding key inside a map data type.
* Example:
```
SELECT 
  CAST(
    MapGet(
      metadata,
      'data1'
      ) AS VARCHAR
    ) AS data1
FROM data_stream
```

#### S2AreaInKm2
* Contract: 
  * **Boolean** `S2AreaInKm2(String s2id)`
* Functionality:
  * Computes the area of an s2 cell in km2
* Example:
```
SELECT
  s2_id,
  S2AreaInKm2(s2_id) AS area,
  event_timestamp
FROM data_stream
```

#### SelectFields
* Contract: 
  * **Object[]** `SelectFields(ByteString[] inputProtoBytes, String protoClassName, String fieldPath) , Object[] SelectFields(List<DynamicMessage> filteredData, String fieldPath)`
* Functionality:
  * This is one of the UDFs related to **LongbowPlus** and has to be used either with **SelectFields** and **Filters** UDFs or alone. Find details on LongbowPlus [here](../advance/longbow_plus.md).
  * Can select a single field from the list of proto bytes output from the LongbowRead phase. Can be used with or without applying filters on top of LongbowRead output(which will be in repeated bytes).
* Example:
  * When used alone:
    ```
    SELECT 
      cardinality(
        SelectFields(
          proto_data,
          input_class_name,
          'order_number'
        )
      )
    FROM data_stream
    ```
  * When used with Filters
    ```
    SELECT 
      SelectFields(
        proto_data,
        input_class_name,
        'order_number'
      ) AS favourite_service_provider_guids
    FROM 
      data_stream
    WHERE 
      cardinality(
        SelectFields(
          Filters(
            proto_data, 
            input_class_name, 
            CondEq('status', 'CUSTOMER_CANCELLED')
          ),
          'order_number' 
        )
      ) > 0
    ```

#### S2Id
* Contract: 
  * **String** `S2Id(Double latitude, Double longitude, int level)`
* Functionality:
  * Computes s2id for given lat, long and level.
* Example:
```
SELECT 
  S2Id(data1.latitude, data1.longitude, 13) AS tag_s2id,
  TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp
FROM 
  data_stream
GROUP BY 
  TUMBLE (rowtime, INTERVAL '60' SECOND),
```

#### SingleFeatureWithType
* Contract: 
  * **Row[]** `SingleFeatureWithType(Object... value)`
* Functionality:
  * This is one of the UDFs related to **Feast**. Find details on Feast [here](https://github.com/feast-dev/feast/tree/master/docs#introduction).
  * Converts the given list of objects to a FeatureRow type with key and values from the first two args from every triplet passed in args and data type according to the third element of the triplet.
* Example:
```
SELECT
  SingleFeatureWithType(
	'data1',
	data1,
	'StringType',
	'data2',
	data2,
	'FloatType'
  ) AS features
FROM
  data_stream
```

#### Split
* Contract: 
  * **String[]** `Split(String inputString, String delimiter)`
* Functionality:
  * Split input string based on input delimiter. The delimiter is a regex string, if you want to split by ".", you should use "\\." or it will give an empty array.
* Example:
```
SELECT 
  SPLIT(data1) as new_data1, 
FROM
  data_stream
```

#### StartOfMonth
* Contract: 
  * **Long** `StartOfMonth(long seconds, String time_zone)`
* Functionality:
  * Calculates the seconds in Unix time for the start of a month of a given timestamp second and timezone.
* Example:
```
SELECT 
  StartOfMonth(
    event_timestamp.seconds,
    'America/New_York'
  ) AS start_of_month
FROM data_stream
```

#### StartOfWeek
* Contract: 
  * **Long** `StartOfWeek(long seconds, String timeZone)`
* Functionality:
  * Calculates the seconds in Unix time for the start of a week of a given timestamp second and timezone.
* Example:
```
SELECT 
  StartOfWeek(
    event_timestamp.seconds,
    'America/New_York'
  ) AS start_of_week
FROM data_stream
```

#### TimeInDate
* Contract: 
  * **Long** `TimeInDate(long event_timestamp, int hour, int minute)`
  * **Long** `TimeInDate(long seconds, int hour, int minute, String time_zone)`
* Functionality:
  * Returns calender's time value in seconds
* Example:
```
SELECT 
  TimeInDate(event_timestamp.seconds, 10, 30, 'America/New_York') AS date
FROM 
  data_stream
```

#### TimestampFromUnix
 * Contract: 
   * **Timestamp** TimestampFromUnix(long seconds) 
 * Functionality:
   * Gets java.sql.Timestamp from UNIX seconds.
 * Example:
```
SELECT 
  TimestampFromUnix(event_timestamp.seconds) AS T
FROM 
  data_stream
```

### Aggregate Functions

#### CollectArray
* Contract: 
  * **`ArrayList<Object>`** `CollectArray(Object obj)`
* Functionality:
  * Return an ArrayList of the objects passed.
* Example:
```
SELECT
  CollectArray(DISTINCT data1) AS data1s,
  TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp
FROM
  data_stream
GROUP BY
  TUMBLE(rowtime, INTERVAL '60' SECOND)
```

#### DistinctCount
* Contract: 
  * **Int** `DistinctCount(String metric)`
* Functionality:
  * Returns the distinct count of a field in the input stream.
* Example:
```
SELECT
  DistinctCount(data1) AS distic_count_data1,
  TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS window_timestamp
FROM
  data_stream
GROUP BY
  TUMBLE (rowtime, INTERVAL '60' SECOND)
```

#### Features
* Contract: 
  * **`Row[]`** `Features(Object... objects)`
* Functionality:
* This is one of the UDFs related to **Feast**. Find details on Feast [here](https://github.com/feast-dev/feast/tree/master/docs#introduction).
  * Converts the given list of objects to a FeatureRow type with key and values from every pair passed in args.
* Example:
```
SELECT
  Features(
    data1,
    data2
  ) AS features,
  TUMBLE_END(rowtime, INTERVAL '1' MINUTE) AS event_timestamp
FROM
  data_stream
GROUP BY
  TUMBLE (rowtime, INTERVAL '1' MINUTE)
```

#### FeaturesWithType
* Contract: 
  * **`Row[]`** `FeaturesWithType(Object... value)  [for FeatureRow]`
* Functionality:
  * This is one of the UDFs related to **Feast**. Find details on Feast [here](https://github.com/feast-dev/feast/tree/master/docs#introduction).
  * Converts the given list of objects to a FeatureRow type with key and values from the first two args from every triplet passed in args and data type according to the third element of the triplet.
* Example:
```
SELECT
  FeaturesWithType(
    'data1', 
    data1,
    'StringType', 
    'data2', 
    data2, 
    'FloatType'
  ) AS features,
FROM
  data_stream
GROUP BY
  TUMBLE(rowtime, INTERVAL '1' MINUTE)
```

#### PercentileAggregator
* Contract: 
  * **Double** `PercentileAggregator(BigDecimal percentile, BigDecimal value)`
* Functionality:
  * Get percentile value.
* Example:
```
SELECT
  PercentileAggregator(
    data1, 
    data2
  ) AS percentile,
FROM
  data_stream
GROUP BY
  TUMBLE(rowtime, INTERVAL '1' MINUTE)
```

### Table Functions

#### HistogramBucket
* Contract: 
  * **Void** `HistogramBucket(double dValue, String buckets)`
* Functionality:
  * Returns buckets for given value to calculate histograms.
* Example:
```
SELECT
  data1, 
  data2,
  data3,
  'buckets', 
FROM 
  data_stream,
LATERAL TABLE(HistogramBucket(data1, 'buckets'));
```

#### OutlierMad
* Contract: 
  * **Void** `OutlierMad(ArrayList<Double> values, ArrayList<Timestamp> timestampsArray, Timestamp windowStartTime, Integer windowLengthInMinutes, Integer observationPeriodInMinutes, Integer tolerance, Integer outlierPercentage)`
* Functionality:
  * Determines outliers for a given time series based on threshold, observation window, and tolerance provided.
* Example:
```
SELECT
  `timestamp` AS window_timestamp,
  `value`,
  `upperBound`,
  `lowerBound`,
  CASE
    WHEN `isOutlier` THEN 1
    ElSE 0
  END as isOutlier
FROM 
  data_stream
LATERAL TABLE(
  OutlierMad(
    values_array,
    timestamps_array,
    hop_start_time,
    CAST(20 AS INTEGER),
    CAST(3 AS INTEGER),
    CAST(10 AS INTEGER),
    CAST(10 AS INTEGER)
  )
) AS T(
  `timestamp`,
  `value`,
  `upperBound`,
  `lowerBound`,
  `isOutlier`
)
```
