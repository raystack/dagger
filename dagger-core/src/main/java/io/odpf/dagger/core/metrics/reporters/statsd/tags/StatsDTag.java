package io.odpf.dagger.core.metrics.reporters.statsd.tags;

import org.apache.flink.util.Preconditions;

public class StatsDTag {
    private final String tagKey;
    private final String tagValue;
    private static final String NIL_TAG_VALUE = "NIL_TAG_VALUE";

    public StatsDTag(String key, String value) {
        validate(key, value);
        this.tagKey = key;
        this.tagValue = value;
    }

    private void validate(String key, String value) {
        Preconditions.checkArgument(key != null && !key.isEmpty(),
                "Tag key cannot be null or empty");
        Preconditions.checkArgument(value != null && !value.isEmpty(),
                "Tag value cannot be null or empty. Refer other constructors for creating a tag with only key");
    }

    public StatsDTag(String tagName) {
        this(tagName, NIL_TAG_VALUE);
    }

    public String getFormattedTag() {
        if (tagValue.equals(NIL_TAG_VALUE)) {
            return tagKey;
        } else {
            return String.format("%s=%s", tagKey, tagValue);
        }
    }
}
