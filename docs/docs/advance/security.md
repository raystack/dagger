# Security

The primary goals of the Dagger security needs are to enable secure data access for jobs within a cluster from source.

# Supported secure data access sources 

We currently support secure data access from kafka source using simple authentication security layer([SASL](https://kafka.apache.org/documentation/#security_sasl)) and SSL encryption for data-in-transit between the source kafka and the dagger client.

- [KAFKA_SOURCE](../guides/choose_source.md)
- [KAFKA_CONSUMER](../guides/choose_source.md)

## Supported SASL authentication methods and mechanisms

SASL (Simple Authentication Security Layer) is a framework that provides developers of applications and shared libraries with mechanisms for authentication, data integrity-checking, and encryption.

Dagger currently support SASL based authentication with `KAFKA_SOURCE` and `KAFKA_CONSUMER` sources using - 

- [SASL/SCRAM](https://kafka.apache.org/documentation/#security_sasl_scram)
- [SASL/PLAIN](https://kafka.apache.org/documentation/#security_sasl_plain)

**Note:** You must configure your Kafka cluster to enable SASL authentication. See the [Kafka documentation](https://kafka.apache.org/documentation/#security_overview) for your Kafka version to learn how to configure SASL authentication.

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

STREAMS config to consume data from the SASL/SCRAM authentication enabled kafka -

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

STREAMS config to consume data from multiple kafka source-

```
STREAMS = [
    {
      "SOURCE_KAFKA_TOPIC_NAMES": "test-topic-0",
      "INPUT_SCHEMA_TABLE": "data_stream_0",
      "INPUT_SCHEMA_PROTO_CLASS": "com.tests.TestMessage",
      "INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX": "41",
      "SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS": "localhost-1:9092",
      "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE": "false",
      "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET": "latest",
      "SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID": "dummy-consumer-group",
      "SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
      "SOURCE_KAFKA_CONSUMER_CONFIG_SASL_MECHANISM": "SCRAM-SHA-512",
      "SOURCE_KAFKA_CONSUMER_CONFIG_SASL_JAAS_CONFIG": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafkaclient1\" password=\"kafkaclient1-secret\";""SOURCE_KAFKA_NAME": "local-kafka-stream",
      "SOURCE_KAFKA_NAME": "local-kafka-stream-0",
      "SOURCE_DETAILS": [
        {
          "SOURCE_TYPE": "UNBOUNDED",
          "SOURCE_NAME": "KAFKA_CONSUMER"
        }
      ]
    },
    {
      "SOURCE_KAFKA_TOPIC_NAMES": "test-topic-1",
      "INPUT_SCHEMA_TABLE": "data_stream_1",
      "INPUT_SCHEMA_PROTO_CLASS": "com.tests.TestMessage",
      "INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX": "41",
      "SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS": "localhost-2:9092",
      "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE": "false",
      "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET": "latest",
      "SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID": "dummy-consumer-group",
      "SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL": "SASL_SSL",
      "SOURCE_KAFKA_CONSUMER_CONFIG_SASL_MECHANISM": "SCRAM-SHA-512",
      "SOURCE_KAFKA_CONSUMER_CONFIG_SASL_JAAS_CONFIG": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafkaclient2\" password=\"kafkaclient2-secret\";",
      "SOURCE_KAFKA_NAME": "local-kafka-stream-1",
      "SOURCE_DETAILS": [
        {
          "SOURCE_TYPE": "UNBOUNDED",
          "SOURCE_NAME": "KAFKA_CONSUMER"
        }
      ]
    },
    {
      "SOURCE_KAFKA_TOPIC_NAMES": "test-topic-2",
      "INPUT_SCHEMA_TABLE": "data_stream-2",
      "INPUT_SCHEMA_PROTO_CLASS": "com.tests.TestMessage",
      "INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX": "41",
      "SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS": "localhost:9092",
      "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE": "false",
      "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET": "latest",
      "SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID": "dummy-consumer-group",
      "SOURCE_KAFKA_NAME": "local-kafka-stream-2", 
      "SOURCE_DETAILS": [
        {
          "SOURCE_TYPE": "UNBOUNDED",
          "SOURCE_NAME": "KAFKA_CONSUMER"
        }
      ]
    }
  ]
```
## Encryption and Authentication using SSL

SSL is used for encryption of traffic as well as authentication. By default, SSL is disabled in dagger kafka source but can be turned on if needed.

Dagger currently support SSL based encryption and authentication with `KAFKA_SOURCE` and `KAFKA_CONSUMER`.

**Note:** You must configure your Kafka cluster to enable encryption and authentication using SSL. See the [Kafka documentation](https://kafka.apache.org/34/documentation.html#security_ssl) for your Kafka version to learn how to configure SSL encryption of traffic as well as authentication.

### Configurations

To consume data from SSL/TLS enabled kafka, following variables need to be configured as part of [STREAMS](../reference/configuration.md) JSON

##### `SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL`

Defines the security protocol used to communicate with SSL enabled kafka. Other than SASL supported values, as mentioned above, Dagger supports,
* `SSL`: to enable SSL/TLS for encryption and authentication

* Example value: `SSL`
* Type: `optional` required only for SSL enabled `KAFKA_CONSUMER` or `KAFKA_SOURCE`

##### `SOURCE_KAFKA_CONSUMER_CONFIG_SSL_PROTOCOL`

Defines the security protocol used to communicate with SSL enabled kafka. Find more details on this config [here](https://kafka.apache.org/documentation/#brokerconfigs_ssl.protocol)
Dagger supported values are: TLSv1.2, TLSv1.3, TLS, TLSv1.1, SSL, SSLv2 and SSLv3

* Example value 1: `SSL`
* Type: `optional` required only for SSL enabled `KAFKA_CONSUMER` or `KAFKA_SOURCE`

* Example value 2: `TLS`
* Type: `optional` required only for TLS enabled `KAFKA_CONSUMER` or `KAFKA_SOURCE`

##### `SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEY_PASSWORD`

Defines the SSL Key Password for Kafka source. Find more details on this config [here](https://kafka.apache.org/documentation/#brokerconfigs_ssl.key.password)

* Example value: `myKeyPass`
* Type: `optional` required only for SSL enabled `KAFKA_CONSUMER` or `KAFKA_SOURCE`

##### `SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_LOCATION`

Defines the SSL KeyStore location or path for Kafka source. Find more details on this config [here](https://kafka.apache.org/documentation/#brokerconfigs_ssl.keystore.location)

* Example value: `/tmp/myKeyStore.jks`
* Type: `optional` required only for SSL enabled `KAFKA_CONSUMER` or `KAFKA_SOURCE`

##### `SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_PASSWORD`

Defines the SSL KeyStore password for Kafka source. Find more details on this config [here](https://kafka.apache.org/documentation/#brokerconfigs_ssl.keystore.password)

* Example value: `myKeyStorePass`
* Type: `optional` required only for SSL enabled `KAFKA_CONSUMER` or `KAFKA_SOURCE`

##### `SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_TYPE`

Defines the SSL KeyStore Type like JKS, PKCS12 etc  for Kafka source. Find more details on this config [here](https://kafka.apache.org/documentation/#brokerconfigs_ssl.keystore.type)
Dagger supported values are: JKS, PKCS12, PEM

* Example value: `JKS`
* Type: `optional` required only for SSL enabled `KAFKA_CONSUMER` or `KAFKA_SOURCE`

##### `SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_LOCATION`

Defines the SSL TrustStore location or path for Kafka source. Find more details on this config [here](https://kafka.apache.org/documentation/#brokerconfigs_ssl.truststore.location)

* Example value: `/tmp/myTrustStore.jks`
* Type: `optional` required only for SSL enabled `KAFKA_CONSUMER` or `KAFKA_SOURCE`

##### `SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_PASSWORD`

Defines the SSL TrustStore password for Kafka source. Find more details on this config [here](https://kafka.apache.org/documentation/#brokerconfigs_ssl.truststore.password)

* Example value: `myTrustStorePass`
* Type: `optional` required only for SSL enabled `KAFKA_CONSUMER` or `KAFKA_SOURCE`

##### `SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_TYPE`

Defines the SSL TrustStore Type like JKS, PKCS12 for Kafka source. Find more details on this config [here](https://kafka.apache.org/documentation/#brokerconfigs_ssl.truststore.type)
Dagger supported values are: JKS, PKCS12, PEM

* Example value: `JKS`
* Type: `optional` required only for SSL enabled `KAFKA_CONSUMER` or `KAFKA_SOURCE`

### Example

STREAMS configurations to consume data from the SSL/TLS enabled kafka -

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
    "SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL": "SSL", 
    "SOURCE_KAFKA_CONSUMER_CONFIG_SSL_PROTOCOL": "TLS",
    "SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_LOCATION": "my-keystore.jks", 
    "SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_PASSWORD": "test-keystore-pass", 
    "SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_TYPE": "JKS", 
    "SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_LOCATION": "my-truststore.jks", 
    "SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_PASSWORD": "test-truststore-pass", 
    "SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_TYPE": "JKS", 
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

STREAMS configurations to consume data from multiple kafka sources -

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
    "SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL": "SSL", 
    "SOURCE_KAFKA_CONSUMER_CONFIG_SSL_PROTOCOL": "TLS",
    "SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_LOCATION": "my-keystore.jks", 
    "SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_PASSWORD": "test-keystore-pass", 
    "SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_TYPE": "JKS", 
    "SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_LOCATION": "my-truststore.jks", 
    "SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_PASSWORD": "test-truststore-pass", 
    "SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_TYPE": "JKS", 
    "SOURCE_KAFKA_NAME": "local-kafka-stream",
    "SOURCE_DETAILS": [
      {
        "SOURCE_TYPE": "UNBOUNDED",
        "SOURCE_NAME": "KAFKA_CONSUMER"
      }
    ]
  },
  {
    "SOURCE_KAFKA_TOPIC_NAMES": "test-topic-2",
    "INPUT_SCHEMA_TABLE": "data_stream-2",
    "INPUT_SCHEMA_PROTO_CLASS": "com.tests.TestMessage",
    "INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX": "41",
    "SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS": "localhost:9091",
    "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE": "false",
    "SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET": "latest",
    "SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID": "dummy-consumer-group",
    "SOURCE_KAFKA_NAME": "local-kafka-stream-2", 
    "SOURCE_DETAILS": [
      {
        "SOURCE_TYPE": "UNBOUNDED",
        "SOURCE_NAME": "KAFKA_CONSUMER"
      }
    ]
  }
]
```
