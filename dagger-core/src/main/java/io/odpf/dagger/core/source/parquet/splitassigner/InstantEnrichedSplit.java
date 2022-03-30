package io.odpf.dagger.core.source.parquet.splitassigner;

import lombok.Getter;
import org.apache.flink.connector.file.src.FileSourceSplit;

import java.time.Instant;

public class InstantEnrichedSplit {
    @Getter
    private final FileSourceSplit fileSourceSplit;
    @Getter
    private final Instant instant;

    public InstantEnrichedSplit(FileSourceSplit fileSourceSplit, Instant instant) {
        this.fileSourceSplit = fileSourceSplit;
        this.instant = instant;
    }
}
