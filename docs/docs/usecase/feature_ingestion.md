# Feature Ingestion

Feature generation is the process of creating new features from some business event or some existing feature(s) potentially for the use in statistical analysis/Machine learning models. With the increasing demand for online machine learning the requirement for an efficient way of real-time Feature generation is at an all-time high.

## How Dagger Solves?

User can define some kind of custom Feature generation rule in dagger from any business events. The Feature generation rule can be some simple Dagger Query or some complex statistical transformation using custom operators through transformers or a combination of both. Why using Dagger as a Feature generation engine easy :

- Reusable templates for generic feature transformation using UDF and Transformer.
- SQL driven Feature logic definition makes defining new features easy for Data Scientists and Analysts. Windowed query helps you defining features in multiple intervals.
- Long windowed Aggregation support(Longbow) makes it easy to create features on some historical Data points which help you in the training and labelling stage.
- Great compatibility with some opensource feature store engine like [Feast](https://github.com/feast-dev/feast).
- Realtime feature generation enables you serving your Machine learning models from the servicing layer in a near realtime.

After the real-time feature is generated they can be pushed to either an offline/batch store for data labelling and training or an online store for model serving.

### Sample Feature Generation

This is a simple Dagger Query that generates and ingests features in a predefined format. The query simply aggregates the `created` events for a `TEST_SERVICE` per locality with 5 minutes of tumble window. The count field is the value/metric and other fields are simple schema definition of features.

```SQL
SELECT
    "sample-feature" as name,
    service_type,
    country_code AS country_code,
    CAST (locality_id as VARCHAR(20)) as locality_id,
    CAST (
        sum(
            case
                when status = 'CREATED' then 1
                else 0
            end
        ) as DOUBLE
    ) as `value`,
    TUMBLE_END(rowtime, INTERVAL '5' MINUTE) AS event_timestamp
FROM
    `test-booking`
where
    service_type = 'TEST_SERVICE'
GROUP BY
    TUMBLE (rowtime, INTERVAL '5' MINUTE),
    locality_information,
    locality_id
```

Sample schema of Feature

```protobuf
message FeatureMessage {
    string name = 1;
    string key = 2;
    double value = 3;
    google.protobuf.Timestamp event_timestamp = 4;
    string country_code = 5;
    string locality_id = 6;
    gojek.esb.types.ServiceType.Enum service_type = 7;
}
```

### Long Windowed Features

In case you want to have features for long windowed Data, like data for weeks/months, which is tricky using the inbuilt local state management. Dagger exposes Longbow for long windowed aggregation which can solve this. Find more information about longbow [here](../advance/longbow.md).

## Integration with Feast

[Feast](https://github.com/feast-dev/feast) is one of the popular Feature Store Engines. Dagger is nicely compatible with feast and can be used to ingest streaming features in a predefined format (FeatureRow or some other format) that can be ingested into Feast's input, Kafka. Some of the prebuilt UDFs and Transformers help you efficiently integrate with Feast. Find more about Feast [here](https://feast.dev/).
