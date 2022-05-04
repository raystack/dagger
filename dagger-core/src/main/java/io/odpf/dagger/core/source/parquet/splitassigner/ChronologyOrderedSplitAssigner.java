package io.odpf.dagger.core.source.parquet.splitassigner;

import io.odpf.dagger.core.source.config.models.TimeRanges;
import io.odpf.dagger.core.source.parquet.path.PathParser;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.text.ParseException;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;

public class ChronologyOrderedSplitAssigner implements FileSplitAssigner {
    private final PriorityBlockingQueue<InstantEnrichedSplit> unassignedSplits;
    private static final int INITIAL_DEFAULT_CAPACITY = 11;
    private PathParser pathParser;
    private TimeRanges timeRanges;

    private ChronologyOrderedSplitAssigner(Collection<FileSourceSplit> fileSourceSplits, PathParser pathParser, TimeRanges timeRanges) {
        this.pathParser = pathParser;
        this.timeRanges = timeRanges;
        this.unassignedSplits = new PriorityBlockingQueue<>(INITIAL_DEFAULT_CAPACITY, getFileSourceSplitComparator());
        for (FileSourceSplit split : fileSourceSplits) {
            validateAndAddSplits(split);
        }
    }

    @Override
    public Optional<FileSourceSplit> getNext(@Nullable String hostname) {
        InstantEnrichedSplit instantEnrichedSplit = unassignedSplits.poll();
        if (instantEnrichedSplit == null) {
            return Optional.empty();
        }
        return Optional.of(instantEnrichedSplit.getFileSourceSplit());
    }

    @Override
    public void addSplits(Collection<FileSourceSplit> splits) {
        for (FileSourceSplit split : splits) {
            validateAndAddSplits(split);
        }
    }

    @Override
    public Collection<FileSourceSplit> remainingSplits() {
        return unassignedSplits
                .stream()
                .map(InstantEnrichedSplit::getFileSourceSplit)
                .collect(Collectors.toList());
    }

    private void validateAndAddSplits(FileSourceSplit split) {
        try {
            Instant instant = pathParser.instantFromFilePath(split.path());
            if (timeRanges == null || timeRanges.contains(instant)) {
                this.unassignedSplits.add(new InstantEnrichedSplit(split, instant));
            }
        } catch (ParseException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    private Comparator<InstantEnrichedSplit> getFileSourceSplitComparator() {
        return (instantEnrichedSplit1, instantEnrichedSplit2) -> {
            Instant instant1 = instantEnrichedSplit1.getInstant();
            Instant instant2 = instantEnrichedSplit2.getInstant();
            if (instant1.isBefore(instant2)) {
                return -1;
            } else if (instant1.isAfter(instant2)) {
                return 1;
            } else {
                return 0;
            }
        };
    }

    public static class ChronologyOrderedSplitAssignerBuilder implements Serializable {
        private PathParser pathParser;
        private TimeRanges parquetFileDateRange;

        public ChronologyOrderedSplitAssignerBuilder addPathParser(PathParser parser) {
            this.pathParser = parser;
            return this;
        }

        public ChronologyOrderedSplitAssignerBuilder addTimeRanges(TimeRanges timeRanges) {
            this.parquetFileDateRange = timeRanges;
            return this;
        }

        public ChronologyOrderedSplitAssigner build(Collection<FileSourceSplit> fileSourceSplits) {
            return new ChronologyOrderedSplitAssigner(fileSourceSplits, pathParser, parquetFileDateRange);
        }
    }
}
