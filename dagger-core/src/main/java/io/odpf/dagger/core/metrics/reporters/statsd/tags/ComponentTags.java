package io.odpf.dagger.core.metrics.reporters.statsd.tags;

public class ComponentTags {
    private static StatsDTag[] parquetReaderTags;
    private static StatsDTag[] splitAssignerTags;
    private static final String COMPONENT_TAG_KEY = "component";

    public static StatsDTag[] getParquetReaderTags() {
        if (parquetReaderTags == null) {
            parquetReaderTags = new StatsDTag[]{
                    new StatsDTag(COMPONENT_TAG_KEY, "parquet_reader"),
            };
        }
        return parquetReaderTags;
    }

    public static StatsDTag[] getSplitAssignerTags() {
        if (splitAssignerTags == null) {
            splitAssignerTags = new StatsDTag[]{
                    new StatsDTag(COMPONENT_TAG_KEY, "split_assigner"),
            };
        }
        return splitAssignerTags;
    }
}
