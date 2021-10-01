---
sidebar_position: 1
---

# Introduction

Dagger aka Data Aggregator is an easy-to-use, configuration over code, cloud-native framework built on top of Apache Flink
for stateful processing of streaming data. With Dagger, you don't need to write custom applications or complicated code to process data in real-time. Instead, you can write SQL queries and UDFs to do the processing and analysis on streaming data.

![](/img/overview.svg)

## Key Features

Discover why to use Dagger

- **Processing:** Dagger can transform, aggregate, join and enrich Protobuf data in real-time.
- **Scale:** Dagger scales in an instant, both vertically and horizontally for high performance streaming sink and zero data drops.
- **Extensibility:** Add your own sink to dagger with a clearly defined interface or choose from already provided ones.
- **Flexibility:** Add custom business logic in form of plugins \(UDFs, Transformers, Preprocessors and Post Processors\) independent of the core logic.
- **Metrics:** Always know what’s going on with your deployment with built-in [monitoring](./reference/metrics.md) of throughput, response times, errors and more.

## Usecases

- [Map reduce with SQL](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html)
- [Aggregation with SQL](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html), [UDFs](./guides/use_udf.md)
- [Enrichment with Post Processors](./advance/post_processor.md)
- [Data Masking with Hash Transformer](./reference/transformers.md#HashTransformer)
- [Data Deduplication with Transformer](./reference/transformers.md#DeDuplicationTransformer)
- [Realtime long window processing with Longbow](./advance/longbow.md)

To know more, follow the detailed [documentation](https://odpf.gitbook.io/dagger).

## Where to go from here

Explore the following resources to get started with Dagger:

- [Guides](./guides/overview.md) provides guidance on [creating Dagger](./guides/overview.md) with different sinks.
- [Concepts](./concepts/overview.md) describes all important Dagger concepts.
- [Advance](./advance/overview.md) contains details regarding advance features of Dagger.
- [Reference](./reference/overview.md) contains details about configurations, metrics and other aspects of Dagger.
- [Contribute](./contribute/contribution.md) contains resources for anyone who wants to contribute to Dagger.
- [Usecase](./usecase/overview.md) describes examples use cases which can be solved via Dagger.
