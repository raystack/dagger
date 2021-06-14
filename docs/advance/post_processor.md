# Introduction
Post Processors give the capability to do custom stream processing after the SQL processing is performed. Complex transformation, enrichment & aggregation use cases are difficult to execute & maintain using SQL. Post Processors solve this problem through code and/or configuration. This can be used to enrich the stream from external sources (HTTP, ElasticSearch, PostgresDB, GRPC), enhance data points using function or query and transform through user-defined code.

## Flow of Execution
In the flow of Post Processors, External Post Processors, Internal Post Processors and Transformers can be applied sequentially via config. The output of one Post Processor will be the input of the next one. The input SQL is executed first before any of the Post Processors and the Post Processors run only on the output of the SQL. Here is an example of a simple use case that can be solved using Post Processor and sample Data flow Diagrams for that.

* Let's assume that you want to find cashback given for a particular order number from an external API endpoint. You can use an HTTP external post-processor for this. Here is a basic Data flow diagram.

<p align="center">
  <img src="../assets/external-http-post-processor.png" width="80%"/>
</p>

* In the above example, assume you also want to output the information of customer_id and amount which are fields from input proto. Internal SQL Post Processor can be used for selecting these fields from the input stream.

<p align="center">
  <img src="../assets/external-internal-post-processor.png" width="80%"/>
</p>

* After getting customer_id, amount, and cashback amount, you may want to round off the cashback amount. For this, you can write a custom transformer which is a simple Java Flink Map function to calculate the round-off amount.

  **Note:** All the above processors are chained sequentially on the output of previous processor. The order of execution is determined via the order provided in json config.

<p align="center">
  <img src="../assets/external-internal-transformer-post-processor.png" width="80%"/>
</p>

## Types of Post Processors
There are three types of Post Processors :
* [External Post Processor](post_processor.md#external-post-processor)
* [Internal Post Processor](post_processor.md#internal-post-processor)
* [Transformers](docs/../../guides/use_transformer.md)

(Post Processors are entirely configuration driven. All the Post Processor related configs should be configured as part of [PRE_PROCESSOR_CONFIG](update link) json under Settings in Dagger creation flow. Multiple Post Processors can be combined in the same configuration and applied to a single Dagger. )

### External Post Processor
External Post Processor is the one that connects to an external data source to fetch data in an async manner and perform enrichment of the stream message. These kinds of Post Processors use Flink’s API for asynchronous I/O with external data stores. For more details on Flink’s Async I/O find the doc [here](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/operators/asyncio.html).
Currently, we are supporting four external sources as part of this.

#### Elasticsearch 
This enables you to enrich the input streams with any information present in some remote [Elasticsearch](https://www.elastic.co/). For example let's say you have payment transaction logs in input stream but user profile information in Elasticsearch, then you can use this post processor to get the profile information in each record.

##### Configuration

Following variables need to be configured as part of [POST_PROCESSOR_CONFIG](update link) json

###### `host`

IP(s) of the nodes/haproxy.

* Example value: `localhost`
* Type: `required`

###### `port`

Port exposed for the same.

* Example value: `9200`
* Type: `required`

###### `endpoint_pattern`

String template for the endpoint. This will be appended to the host to create the final URL.

* Example value: `/customers/customer/%s`
* Type: `required`

###### `endpoint_variables`

Comma-separated list of variables used for populating identifiers in endpoint_pattern.

* Example value: `customer_id`
* Type: `optional`

###### `retain_response_type`

If true it will not cast the response from ES to output proto schema. The default behaviour is to cast the response to the output proto schema.

* Example value: `false`
* Type: `optional`
* Default value: `false`

###### `retry_timeout`

Timeout between request retries.

* Example value: `5000`
* Type: `optional`

###### `socket_timeout`

The time waiting for data after establishing the connection; maximum time of inactivity between two data packets.

* Example value: `6000`
* Type: `optional`

###### `connect_timeout`

Timeout value for ES client.

* Example value: `5000`
* Type: `optional`

###### `capacity`

This parameter(Async I/O capacity) defines how many asynchronous requests may be in progress at the same time.

* Example value: `30`
* Type: `optional`

###### `output_mapping`

Mapping of fields in output Protos goes here. Based on which part of the response data to use, you can configure the path, and output message fields will be populated accordingly.

* Example value: `{"customer_profile":{ "path":"$._source"}}`
* Type: `required`

##### Sample Query
You can select the fields that want to get from input stream or you want to use for making the request.
  ```SQL
  SELECT customer_id from `booking`
  ```

##### Sample Configuration
  ```properties
  POST_PROCESSOR_ENABLED = true
  POST_PROCESSOR_CONFIG = {
    "external_source": {
      "es": [
        {
          "host": "127.0.0.1",
          "port": "9200",
          "endpoint_pattern": "/customers/customer/%s",
          "endpoint_variables": "customer_id",
          "retry_timeout": "5000",
          "socket_timeout": "6000",
          "stream_timeout": "5000",
          "connect_timeout": "5000",
          "capacity": "30",
          "output_mapping": {
            "customer_profile": {
              "path": "$._source"
            }
          }
        }
      ]
    }
  }
  ```

### Internal Post Processor

## Post Processor requirements

Some basic information you need to know before the creation of a Post Processor Dagger is as follow

### Number of Post Processors
Any number of Post Processors can be added based on the use-case. And also there can be multiple Post Processors of the same type. The initial SQL should not depend on the number of Post Processors and you can simply start with selecting as many fields that are required for the final result as well as the Post Processors in the SQL.

### Throughput
The throughput depends on the input topic of Dagger and after SQL filtering, the enrichment store should be able to handle that load.

### Output Proto
The output proto should have all the fields that you want to output from the input stream as well as fields getting enriched from the Post Processor with the correct data type. For example in the sample Post Processor given here, the output proto should contain the fields like customer_id, round_off_amount, and cashback. Here the later two fields are enriched by the Post Processor while the customer_id is just selected from the input topic. The types of enrichment fields are provided as part of the configuration.

### Connectivity
The enrichment store should have connectivity to the Dagger deployment.
