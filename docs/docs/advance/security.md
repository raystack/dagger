# Security

The primary goals of the Dagger security needs are to enable secure data access for jobs within a cluster from source.

# Supported secure data access sources 

We currently support only secure data access from kafka source using simple authentication security layer([SASL](https://docs.confluent.io/platform/current/kafka/overview-authentication-methods.html#))

- [KAFKA_SOURCE](../guides/choose_source.md)
- [KAFKA_CONSUMER](../guides/choose_source.md)

## Supported SASL authentication methods 

SASL (Simple Authentication Security Layer) is a framework that provides developers of applications and shared libraries with mechanisms for authentication, data integrity-checking, and encryption.
We currently support SASL based authentication for `KAFKA_SOURCE` and `KAFKA_CONSUMER` source using - 

- [SASL/SCRAM](https://docs.confluent.io/platform/current/kafka/authentication_sasl/authentication_sasl_scram.html#clients)
- [SASL/PLAIN](https://docs.confluent.io/platform/current/kafka/authentication_sasl/authentication_sasl_plain.html#clients)

### Configurations

To consume data from SASL/SCRAM or SASL/PLAIN authentication enabled kafka, following variables need to be configured as part of [STREAMS](../reference/configuration.md) JSON

##### `SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL`

Defines the security protocol used to communicate with ACL enabled kafka. Dagger supported values are -
1. SASL_SSL: if SSL encryption is enabled
2. SASL_PLAINTEXT: if SSL encryption is not enabled

* Example value: `SASL_PLAINTEXT`
* Type: `optional` required only for ACL enabled `KAFKA_CONSUMER` or `KAFKA_SOURCE`

##### `SOURCE_KAFKA_CONSUMER_CONFIG_SASL_MECHANISM`

Defines the Simple Authentication and Security Layer (SASL) mechanism used for client connections with ACL enabled kafka. Dagger supported values are: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.
1. Configure PLAIN, if kafka cluster is configured with simple username/password authentication mechanism.
2. Configure SCRAM-SHA-256 or SCRAM-SHA-512 if kafka cluster is configured with Salted Challenge Response Authentication Mechanism (SCRAM).

* Example value: `SCRAM-SHA-512`
* Type: `optional` required only for ACL enabled `KAFKA_CONSUMER` or `KAFKA_SOURCE`

##### `SOURCE_KAFKA_CONSUMER_CONFIG_SASL_JAAS_CONFIG`

Defines the SASL Java Authentication and Authorization Service (JAAS) Config used for JAAS login context parameters for SASL connections in the format used by JAAS configuration files. JAAS configuration file format is described [here](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html).
There are two ways to configure `KAFKA_CONSUMER` or `KAFKA_SOURCE` to provide the necessary information for JAAS:
1. Specify the JAAS configuration using the `SOURCE_KAFKA_CONSUMER_CONFIG_SASL_JAAS_CONFIG` configuration property
2. Pass a static JAAS configuration file into the flink-conf.yaml using the `env.java.opts: -Djava.security.auth.login.config=<jaas_file_path>/<jaas_file_name>` property, at runtime dagger job will use static JAAS config details configured with the flink TaskManager JVM.

If a Dagger specifies both the client property `SOURCE_KAFKA_CONSUMER_CONFIG_SASL_JAAS_CONFIG` and the static JAAS configuration system property `java.security.auth.login.config`, then the client property `SOURCE_KAFKA_CONSUMER_CONFIG_SASL_JAAS_CONFIG` will be used.

* Example value: `org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin";`
* Type: `optional` required only for ACL enabled `KAFKA_CONSUMER` or `KAFKA_SOURCE` if static JAAS configuration system property `java.security.auth.login.config` is not configured in flink cluster.


### Example

To consume data from SASL/SCRAM authentication enabled a kafka -

```
STREAMS = [
  {
    "SOURCE_KAFKA_TOPIC_NAMES": "test-topic",
    "INPUT_SCHEMA_TABLE": "data_stream",
    "INPUT_SCHEMA_PROTO_CLASS": "com.tests.TestMessage",
    "INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX": "41",
    "SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS": "localhost:9092",
    "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE": "false",
    "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET": "latest",
    "SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID": "dummy-consumer-group",
    "SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
    "SOURCE_KAFKA_CONSUMER_CONFIG_SASL_MECHANISM": "SCRAM-SHA-512",
    "SOURCE_KAFKA_CONSUMER_CONFIG_SASL_JAAS_CONFIG": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafkaclient1\" password=\"kafkaclient1-secret\";",
    "SOURCE_KAFKA_NAME": "local-kafka-stream",
    "SOURCE_DETAILS": [
      {
        "SOURCE_TYPE": "UNBOUNDED",
        "SOURCE_NAME": "KAFKA_CONSUMER"
      }
    ]
  }
]
```

To consume data from multiple SASL/SCRAM authentication enabled a kafka source-

```
STREAMS = [
    {
      "SOURCE_KAFKA_TOPIC_NAMES": "test-topic",
      "INPUT_SCHEMA_TABLE": "data_stream",
      "INPUT_SCHEMA_PROTO_CLASS": "com.tests.TestMessage",
      "INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX": "41",
      "SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS": "localhost:9092",
      "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE": "false",
      "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET": "latest",
      "SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID": "dummy-consumer-group",
      "SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
      "SOURCE_KAFKA_CONSUMER_CONFIG_SASL_MECHANISM": "SCRAM-SHA-512",
      "SOURCE_KAFKA_CONSUMER_CONFIG_SASL_JAAS_CONFIG": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafkaclient1\" password=\"kafkaclient1-secret\";""SOURCE_KAFKA_NAME": "local-kafka-stream",
      "SOURCE_DETAILS": [
        {
          "SOURCE_TYPE": "UNBOUNDED",
          "SOURCE_NAME": "KAFKA_CONSUMER"
        }
      ]
    },
    {
      "SOURCE_KAFKA_TOPIC_NAMES": "test-topic",
      "INPUT_SCHEMA_TABLE": "data_stream",
      "INPUT_SCHEMA_PROTO_CLASS": "com.tests.TestMessage",
      "INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX": "41",
      "SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS": "localhost:9092",
      "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE": "false",
      "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET": "latest",
      "SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID": "dummy-consumer-group",
      "SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL": "SASL_SSL",
      "SOURCE_KAFKA_CONSUMER_CONFIG_SASL_MECHANISM": "SCRAM-SHA-512",
      "SOURCE_KAFKA_CONSUMER_CONFIG_SASL_JAAS_CONFIG": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafkaclient2\" password=\"kafkaclient2-secret\";",
      "SOURCE_KAFKA_NAME": "local-kafka-stream",
      "SOURCE_DETAILS": [
        {
          "SOURCE_TYPE": "UNBOUNDED",
          "SOURCE_NAME": "KAFKA_CONSUMER"
        }
      ]
    }
  ]
```