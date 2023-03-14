package com.gotocompany.dagger.core.metrics.reporters.statsd.tags;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class StatsDTagTest {

    @Test
    public void shouldReturnFormattedTagWithKeyValueSeparatedByDelimiter() {
        StatsDTag statsDTag = new StatsDTag("tag-key", "tag-value");

        assertEquals("tag-key=tag-value", statsDTag.getFormattedTag());
    }

    @Test
    public void shouldReturnFormattedTagWithOnlyTagKey() {
        StatsDTag statsDTag = new StatsDTag("my-tag");

        assertEquals("my-tag", statsDTag.getFormattedTag());
    }

    @Test
    public void shouldReturnFormattedTagWithOnlyTagKeyWhenTagValueIsNull() {
        StatsDTag statsDTag = new StatsDTag("my-tag", null);

        assertEquals("my-tag", statsDTag.getFormattedTag());
    }

    @Test
    public void shouldReturnFormattedTagWithOnlyTagKeyWhenTagValueIsEmpty() {
        StatsDTag statsDTag = new StatsDTag("my-tag", "");

        assertEquals("my-tag", statsDTag.getFormattedTag());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenTagKeyIsNull() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> new StatsDTag(null, "tag-value"));

        assertEquals("Tag key cannot be null or empty", ex.getMessage());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenTagKeyIsEmpty() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> new StatsDTag("", "tag-value"));

        assertEquals("Tag key cannot be null or empty", ex.getMessage());
    }
}
