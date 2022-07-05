package io.odpf.dagger.core.source.parquet.splitassigner;

import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.core.metrics.reporters.statsd.manager.DaggerGaugeManager;
import io.odpf.dagger.core.metrics.reporters.statsd.tags.StatsDTag;
import io.odpf.dagger.core.exception.PathParserNotProvidedException;
import io.odpf.dagger.core.source.config.models.TimeRangePool;
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

import static io.odpf.dagger.core.metrics.aspects.ChronologyOrderedSplitAssignerAspects.SPLITS_AWAITING_ASSIGNMENT;
import static io.odpf.dagger.core.metrics.aspects.ChronologyOrderedSplitAssignerAspects.TOTAL_SPLITS_DISCOVERED;
import static io.odpf.dagger.core.metrics.aspects.ChronologyOrderedSplitAssignerAspects.TOTAL_SPLITS_RECORDED;
import static io.odpf.dagger.core.metrics.reporters.statsd.tags.ComponentTags.getSplitAssignerTags;

public class ChronologyOrderedSplitAssigner implements FileSplitAssigner {
    private final PriorityBlockingQueue<InstantEnrichedSplit> unassignedSplits;
    private static final int INITIAL_DEFAULT_CAPACITY = 11;
    private PathParser pathParser;
    private TimeRangePool timeRangePool;
    private DaggerGaugeManager daggerGaugeManager;

    private ChronologyOrderedSplitAssigner(Collection<FileSourceSplit> fileSourceSplits, PathParser pathParser,
                                           TimeRangePool timeRangePool, SerializedStatsDReporterSupplier statsDReporterSupplier) {
        this.registerTagsWithMeasurementManagers(statsDReporterSupplier);
        daggerGaugeManager.registerLong(TOTAL_SPLITS_DISCOVERED, fileSourceSplits.size());
        this.pathParser = pathParser;
        this.timeRangePool = timeRangePool;
        this.unassignedSplits = new PriorityBlockingQueue<>(INITIAL_DEFAULT_CAPACITY, getFileSourceSplitComparator());
        for (FileSourceSplit split : fileSourceSplits) {
            validateAndAddSplits(split);
        }
        daggerGaugeManager.registerLong(TOTAL_SPLITS_RECORDED, unassignedSplits.size());
        daggerGaugeManager.registerLong(SPLITS_AWAITING_ASSIGNMENT, unassignedSplits.size());
    }

    private void registerTagsWithMeasurementManagers(SerializedStatsDReporterSupplier statsDReporterSupplier) {
        StatsDTag[] splitAssignerTags = getSplitAssignerTags();
        daggerGaugeManager = new DaggerGaugeManager(statsDReporterSupplier);
        daggerGaugeManager.register(splitAssignerTags);
    }

    @Override
    public Optional<FileSourceSplit> getNext(@Nullable String hostname) {
        InstantEnrichedSplit instantEnrichedSplit = unassignedSplits.poll();
        if (instantEnrichedSplit == null) {
            return Optional.empty();
        }
        daggerGaugeManager.registerLong(SPLITS_AWAITING_ASSIGNMENT, unassignedSplits.size());
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
            if (timeRangePool == null || timeRangePool.contains(instant)) {
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
        private TimeRangePool parquetFileDateRange;
        private SerializedStatsDReporterSupplier supplier;

        public ChronologyOrderedSplitAssignerBuilder addPathParser(PathParser parser) {
            this.pathParser = parser;
            return this;
        }

        public ChronologyOrderedSplitAssignerBuilder addTimeRanges(TimeRangePool timeRangePool) {
            this.parquetFileDateRange = timeRangePool;
            return this;
        }

        public ChronologyOrderedSplitAssignerBuilder addStatsDReporterSupplier(SerializedStatsDReporterSupplier statsDReporterSupplier) {
            this.supplier = statsDReporterSupplier;
            return this;
        }

        public ChronologyOrderedSplitAssigner build(Collection<FileSourceSplit> fileSourceSplits) {
            if (pathParser == null) {
                throw new PathParserNotProvidedException("Path parser is null");
            }
            return new ChronologyOrderedSplitAssigner(fileSourceSplits, pathParser, parquetFileDateRange, supplier);
        }
    }
}
