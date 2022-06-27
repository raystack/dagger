package io.odpf.dagger.core.metrics.telemetry.statsd;

import io.odpf.dagger.common.metrics.type.statsd.tags.StatsDTag;

public class ComponentTags {
    public static StatsDTag[] getParquetReaderTags() {
        return new StatsDTag[]{
                new StatsDTag("component", "parquet_reader"),
                new StatsDTag("dagger_source", "parquet_source")
        };
    }
}
