# ADD UDF

Want a function to use in SQL which is not supported both by Flink and one of the pre-supported udfs? You can simply write your User-Defined function and contribute to the dagger. Read more on how to use UDFs [here](../guides/use_udf.md).

`Note`: _Please go through the [Contribution guide](../contribute/contribution.md) to know about all the conventions and practices we tend to follow and to know about the contribution process to the dagger._

For adding custom UDFs follow these steps

- Ensure none of the [built-in functions](https://ci.apache.org/projects/flink/flink-docs-master/docs/dev/table/functions/systemfunctions) or [existing UDF](../reference/udfs.md) suits your requirement.
- For adding a UDF, figure out which type of UDF you required. Flink supports three types of [User defined function](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/udfs.html). Choose one of them according to the requirement.

- For getting more insights on writing your UDF, follow [this](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/udfs.html) to create a UDF. It needs to be written in Java/Scala.

- UDF need to be the `function-type` directory inside [this](https://github.com/odpf/dagger/tree/main/dagger-functions/src/main/java/io/odpf/dagger/functions/udfs) on `dagger-functions` subproject.

- Extend either of [ScalarUdf](https://github.com/odpf/dagger/blob/main/dagger-common/src/main/java/io/odpf/dagger/common/udfs/ScalarUdf.java), [TableUdf](https://github.com/odpf/dagger/blob/main/dagger-common/src/main/java/io/odpf/dagger/common/udfs/TableUdf.java) or [AggregateUdf](https://github.com/odpf/dagger/blob/main/dagger-common/src/main/java/io/odpf/dagger/common/udfs/AggregateUdf.java) from `dagger-common`. They are boilerplate contracts extending Flink UDF classes. These classes do some more preprocessing(like exposing some metrics) in the `open` method behind the scene.

- Register the UDF in [this](https://github.com/odpf/dagger/blob/main/dagger-functions/src/main/java/io/odpf/dagger/functions/udfs/factories/FunctionFactory.java) class. This is required to let Flink know about your function.

- Bump up the version and raise a PR for the same. Also please add the registered function to the [list of udfs doc](../reference/udfs.md).

- If you have some business-specific use-cases and you don't want to add UDFs to the open-sourced repo, you can have a separate local codebase for those UDFs. Those UDFs need to be registered in a similar class like the [`UDFFactory`](https://github.com/odpf/dagger/blob/main/dagger-common/src/main/java/io/odpf/dagger/common/udfs/UdfFactory.java). Keep both the UDF classes and the factory class in the classpath of Dagger. Configure the fully qualified Factory class in the `FUNCTION_FACTORY_CLASSES` parameter and you will be able to use the desired UDF in your query.

In the subsequent release of the dagger, your functions should be useable in the query.
