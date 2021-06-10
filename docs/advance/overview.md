# Overview

The following section covers advance features of Dagger.

### [Pre Processor](docs/advance/../../pre_processor.md)

Pre processors enable you to add Flink operators/transformations before passing on the stream to the SQL query.

### [Post Processor](docs/advance/../../post_processor.md)

Post Processors give the capability to do custom stream processing after the SQL processing is performed. Complex transformation, enrichment & aggregation use cases are difficult to execute & maintain using SQL. Post Processors solve this problem through code and/or configuration.

### [Longbow](docs/advance/../../longbow.md)

Longbow enables you to perform large windowed aggregation. It used [Bigtable](https://cloud.google.com/bigtable) for state management.

### [DARTS](docs/advance/../../DARTS.md)

DARTS allows you to join streaming data from a reference data store. It supports reference data store in the form of a list or <key, value> map. It enables the refer-table with the help of UDFs that can be used in the SQL query.
