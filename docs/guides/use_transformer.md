# Use Transformer

Dagger exposes a `configuration + code` driven framework for defining complex Map-Reduce type Flink code which enables dagger to process data beyond SQL.We call them Transformers.
In this section we will know more about transformers, how to use them and how you can create your own transformers matching your use case.

## What is Transformer

- During stream processing there is only so many things you can do using SQL and after a while it becomes a bottleneck to solve complex problems. Though dagger is developed keeping SQL first in mind we realized the necessity to have some other ways to expose some of the complex flink features like async processing, ability to write custom operator(classic Map-Reduce type functions) etc.

- We developed a configuration driven framework called processors which in another way of processing data in addition to SQL. Processors are really extensible and can be applied on the stream before SQL execution (called pre-processor; ideal for complex filtration) or after SQL execution (called post-processor; ideal for async external processing and complex aggregations). Find more about Processors [here](update-link).

- [Transformers](update-link) are a type of processors which let users define more complex processing capability by defining custom Java code. With transformers all the [Flink Operators](https://ci.apache.org/projects/flink/flink-docs-master/docs/dev/datastream/operators/overview/) and granularity of [Flink process Function](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/datastream/operators/process_function/) are supported out of the box. This let users to solve some of the more complex business specific problems.

- Transformer are single stream operations transforming one stream to other. So there are some inherent limitations like multi stream operations in transformations. But there can be multiple transformers as well. They will be processed sequentially.

## Explore Pre-built Transformer

- There are some transformers to solve some generic use cases pre-built in dagger.

- All the pre-supported transformers present in the `dagger-functions` sub-module in [this](https://github.com/odpf/dagger/tree/main/dagger-functions/src/main/java/io/odpf/dagger/functions/transformers) directory. Find more details about each of the existing transformers and some sample examples [here](update-link).

- Incase any of the predefined transformer do not meet your requirement, you can create your custom Transformers by extending some contract. Follow this[contribution guidelines](update-link) on how to add a transformer in dagger.

## Use a Transformer

- Below is a sample query and post-processor configuration for a dagger which uses transformers. As you can see transformers need to be a part of processor (more specifically Post processor in this example).
  ```properties
  SQL_QUERY = "SELECT data_1, data_2, event_timestamp from data_stream"
  POST_PROCESSOR_ENABLED = true.
  POST_PROCESSOR_CONFIG = {
      "internal_source": [
          {
              "output_field": "*",
              "value": "*",
              "type": "sql"
          }
      ],
      "transformers": [
          {
              "transformation_class": "io.odpf.dagger.functions.transformers.SQLTransformer",
              "transformation_arguments": {
                  "sqlQuery": "SELECT count(distinct data_1) AS `count`,data_2, TUMBLE_END(rowtime, INTERVAL '60' SECOND) AS event_timestamp FROM data_stream group by TUMBLE (rowtime, INTERVAL '60' SECOND), data_2"
              }
          }
      ]
  }
  ```
- In the example the internal source just says to select all the fields as selected from the SQL query. Find more about the `internal_source` config parameter [here](update-link).
- The transformer essentially need only a couple of config parameters to work. Provide the fully qualified path of the transformer class is part of `transformation_class` config.
- The other parameter `transformation_arguments` is a map of string and data types where you can put the parameters to be passed to the transformer class as a key value pair.
- For more information about all existing transformers and their functionalities have a look [here](update-link).
