package io.odpf.dagger.core.metrics.reporters.statsd.tags;

public class ComponentTags {
    private static StatsDTag[] parquetReaderTags;
    private static StatsDTag[] splitAssignerTags;
    private static final String COMPONENT_TAG_KEY = "component";
    private static final String DAGGER_SOURCE_TAG_KEY = "dagger_source";

    public static StatsDTag[] getParquetReaderTags() {
        if (parquetReaderTags == null) {
            parquetReaderTags = new StatsDTag[]{
                    new StatsDTag(COMPONENT_TAG_KEY, "parquet_reader"),
                    new StatsDTag(DAGGER_SOURCE_TAG_KEY, "parquet_source")
            };
        }
        return parquetReaderTags;
    }

    public static StatsDTag[] getSplitAssignerTags() {
        if (splitAssignerTags == null) {
            splitAssignerTags = new StatsDTag[]{
                    new StatsDTag(COMPONENT_TAG_KEY, "split_assigner"),
                    new StatsDTag(DAGGER_SOURCE_TAG_KEY, "parquet_source")
            };
        }
        return splitAssignerTags;
    }
}
