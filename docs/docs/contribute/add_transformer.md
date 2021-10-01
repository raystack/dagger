# ADD Transformer

The Transformers give Dagger the capability for applying any custom _transformation logic_ user wants on Data(pre or post aggregated). In simple terms, Transformers are user-defined Java code that can do transformations like Map, Filter, Flat maps etc. Find more information on Transformers here.

Many Transformation logics are pre-supported in Dagger. But since Transformers are more advanced ways of injecting business logic as plugins to Dagger, there can be cases when existing Transformers are not sufficient for user requirement. This section documents how you can add Transformers to dagger.

For adding custom Transformers follow these steps

- Ensure none of the [built-in Transformers](../reference/transformers.md) suits your requirement.

- Transformers take [StreamInfo](https://github.com/odpf/dagger/blob/main/dagger-common/src/main/java/io/odpf/dagger/common/core/StreamInfo.java) which is a wrapper around Flink DataStream as input and transform them to some other StreamInfo/DataStream.

- To define a new Transformer implement Transformer interface. The contract of Transformers is defined [here](https://github.com/odpf/dagger/blob/main/dagger-common/src/main/java/io/odpf/dagger/common/core/Transformer.java).

- Since an input DataStream is available in Transformer, all the Flink supported operators which transform `DataStream -> DataStream` can be applied/used by default for the transformations. Operators are how Flink exposes classic Map-reduce type functionalities. Read more about Flink Operators [here](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/operators/).

- In the case of single Operator Transformation you can extend the desired Operator in the Transformer class itself. For example, follow this code of [HashTransformer](https://github.com/odpf/dagger/blob/main/dagger-functions/src/main/java/io/odpf/dagger/functions/transformers/HashTransformer.java). You can also define multiple chaining operators to Transform Data.

- A configuration `transformation_arguments` inject the required parameters as a Constructor argument to the Transformer class. From the config point of view, these are simple Map of String and Object. So you need to cast them to your desired data types. Find a more detailed overview of the transformer example [here](../guides/use_transformer.md).

- Transformers are injected into the `dagger-core` during runtime using Java reflection. So unlike UDFs, they don't need registration. You just need to mention the fully qualified Transformer Java class Name in the configurations.

- Bump up the version and raise a PR for the Transformer. Also please add the Transformer to the [list of Transformers doc](../reference/transformers.md). Once the PR gets merged the transformer should be available to be used.

- If you have specific use-cases you are solving using Transformers and you don't want to add them to the open-source repo, you can have a separate local codebase for those Transformers and add it to the classpath of the dagger. With the correct Transformation configurations, they should be available to use out of the box.

`Note`: _Please go through the [Contribution guide](../contribute/contribution.md) to know about all the conventions and practices we tend to follow and to know about the contribution process to the dagger._
