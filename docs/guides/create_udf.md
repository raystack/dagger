# Create UDF

Want a function to use in SQL but neither Flink nor one of pre-supported udfs do not meetˀ the requirement. You can simply write your own User Defined function and contribute to dagger.

`Note` : _Please go through the Contribution guide to know about all the conventions and practices we tend to follow and to know about the contribution process to dagger._

For adding custom UDFs follow these steps

- For adding a UDF, figure out which type of UDF you required. Flink supports three types of [User defined function](https://ci.apache.org/projects/flink/flink-docs-master/docs/dev/table/functions/udfs/). Choose one of them according to the requirement.

- For getting more insights on writing your own UDF, follow [this](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/udfs.html) to create a UDF. It needs to be written in Java.

- UDF need to be the function-type directory inside [this](https://github.com/odpf/dagger/tree/main/dagger-functions/src/main/java/io/odpf/dagger/functions/udfs) on `dagger-functions` subproject.

- Extend either of ScalarUdf, TableUdf or AggregateUdf from `dagger-common`. They are boilerplate contracts on flink supported interfaces.

- Manually Register the UDF in [this](https://github.com/odpf/dagger/blob/main/dagger-functions/src/main/java/io/odpf/dagger/functions/udfs/factories/FunctionFactory.java) class. This is required to let Flink know about your function.

- Create a UDF on this repo and raise a PR for the same. Also please add the registered function to the [list of udfs doc][update link].

In the subsequent release of dagger your functions should be useable in the query.
