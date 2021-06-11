# Introduction
Post Processors give the capability to do custom stream processing after the SQL processing is performed. Complex transformation, enrichment & aggregation use cases are difficult to execute & maintain using SQL. Post Processors solve this problem through code and/or configuration. This can be used to enrich the stream from external sources (HTTP, ElasticSearch, PostgresDB, GRPC), enhance data points using function or query and transform through user-defined code.

## Flow of Execution
In the flow of Post Processors, External Post Processors, Internal Post Processors and Transformers can be applied sequentially via config. The output of one Post Processor will be the input of the next one. The input SQL is executed first before any of the Post Processors and the Post Processors run only on the output of the SQL. Here is an example of a simple use case that can be solved using Post Processor and sample Data flow Diagrams for that.

* Let's assume that you want to find cashback given for a particular order number from an external API endpoint. You can use an HTTP external post-processor for this. Here is a basic Data flow diagram.

<p align="center">
  <img src="../assets/external-http-post-processor.png" width="70%"/>
</p>

* From the previous example, assume you want the information of customer_id and amount which are fields from input proto. Internal SQL Post Processor can be used for selecting these fields from the input stream.

<p align="center">
  <img src="../assets/external-internal-post-processor.png" width="70%"/>
</p>

* After getting customer_id, amount, and cashback amount, you may want to round off the cashback amount. For this, you can write a custom transformer which is a simple Java Flink Map function to calculate the round-off amount. Given a simple Data flow diagram for this.

## Types of Post Processors
There are three types of Post Processors :
* [External Post Processor](post_processor.md#external-post-processor)
* [Internal Post Processor](post_processor.md#internal-post-processor)
* [Transformers](docs/../../guides/use_transformer.md)

(Post Processors are entirely configuration driven. All the Post Processor related configs should be configured as part of [PRE_PROCESSOR_CONFIG](update link) json under Settings in Dagger creation flow. Multiple Post Processors can be combined in the same configuration and applied to a single Dagger. )

### External Post Processor

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
