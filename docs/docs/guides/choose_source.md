# Choosing a Source

Dagger currently supports 3 kinds of data sources:

## `KAFKA_SOURCE` and `KAFKA_CONSUMER`

Both these sources use [Kafka](https://kafka.apache.org/) as the source of data. `KAFKA_SOURCE` uses Flink's [Kafka Source](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/datastream/kafka/#kafka-source) streaming 
connector, built using Flink's Data Source API. `KAFKA_CONSUMER` is based on the now deprecated Flink
[Kafka Consumer](https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/connectors/kafka.html#kafka-consumer) 
streaming connector.

They are used for unbounded data streaming use cases, that is, for operating on data generated in real time. In order to 
configure any of these, you would need to set up Kafka(1.0+) either in a local or clustered environment. Follow this 
[quick start](https://kafka.apache.org/quickstart) to set up Kafka in the local machine. If you have a clustered Kafka 
you can configure it to use in Dagger directly.

## `PARQUET_SOURCE`

This source uses Parquet files as the source of data. It is useful for bounded data streaming use cases, that is, 
for processing data as a stream from historically generated Parquet Files of fixed size. The parquet files can be either hourly 
partitioned, such as

```text
root_folder
    - booking_log
        - dt=2022-02-05
            - hr=09
                * g6agdasgd6asdgvadhsaasd829ajs.parquet
                * . . . (more parquet files)
            - (...more hour folders)
        - (... more date folders)

```

or date partitioned, such as:

```text
root_folder
    - shipping_log
        - dt=2021-01-11
            * hs7hasd6t63eg7wbs8swssdasdasdasda.parquet
            * ...(more parquet files)
        - (... more date folders)

```

The file paths can be either in the local file system or in GCS bucket. When parquet files are provided from GCS bucket,
Dagger will require a `core_site.xml` to be configured in order to connect and read from GCS. A sample `core_site.xml` is
present in dagger and looks like this:

```xml
<configuration>
    <property>
        <name>google.cloud.auth.service.account.enable</name>
        <value>true</value>
    </property>
    <property>
        <name>google.cloud.auth.service.account.json.keyfile</name>
        <value>/Users/dummy/secrets/google_service_account.json</value>
    </property>
    <property>
        <name>fs.gs.requester.pays.mode</name>
        <value>CUSTOM</value>
        <final>true</final>
    </property>
    <property>
        <name>fs.gs.requester.pays.buckets</name>
        <value>my_sample_bucket_name</value>
        <final>true</final>
    </property>
    <property>
        <name>fs.gs.requester.pays.project.id</name>
        <value>my_billing_project_id</value>
        <final>true</final>
    </property>
</configuration>
```

You can look into the official [GCS Hadoop Connectors](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md)
documentation to know more on how to edit this xml as per your needs.