package org.raystack.dagger.core.metrics.aspects;

import org.raystack.dagger.common.metrics.aspects.AspectType;
import org.raystack.dagger.common.metrics.aspects.Aspects;

public enum ParquetReaderAspects implements Aspects {
    READER_CREATED("reader_created", AspectType.Counter),
    READER_CLOSED("reader_closed", AspectType.Counter),
    READER_ROWS_EMITTED("reader_rows_emitted", AspectType.Counter),
    READER_ROW_DESERIALIZATION_TIME("reader_row_deserialization_time", AspectType.Histogram),
    READER_ROW_READ_TIME("reader_row_read_time", AspectType.Histogram);

    private final String value;
    private final AspectType aspectType;

    ParquetReaderAspects(String value, AspectType aspectType) {
        this.value = value;
        this.aspectType = aspectType;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public AspectType getAspectType() {
        return aspectType;
    }
}
