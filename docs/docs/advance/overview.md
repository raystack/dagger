# Overview

The following section covers advance features of Dagger.

### [Pre Processor](./pre_processor.md)

Pre processors enable you to add Flink operators/transformations before passing on the stream to the SQL query.

### [Post Processor](./post_processor.md)

Post Processors give the capability to do custom stream processing after the SQL processing is performed. Complex transformation, enrichment & aggregation use cases are difficult to execute & maintain using SQL. Post Processors solve this problem through code and/or configuration.

### [Longbow](./longbow.md)

Longbow enables you to perform large windowed aggregation. It uses [Bigtable](https://cloud.google.com/bigtable) for managing historical data and SQL based DSL for configuration.

### [Longbow+](./longbow_plus.md)

Longbow+ is an enhanced version of longbow. It has additional support for complex data types for long windowed aggregations.

### [DARTS](./DARTS.md)

DARTS allows you to join streaming data from a reference data store. It supports reference data store in the form of a list or <key, value> map. It enables the refer-table with the help of UDFs which can be used in the SQL query. Currently we only support GCS as reference data source.

### [Security](./security.md)

Enable secure data access from ACL enabled kafka source using SASL (Simple Authentication Security Layer) authentication. Also enable data access from SSL/TLS enabled kafka source.