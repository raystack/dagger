package io.odpf.dagger.core.source.parquet.splitassigner;

import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.core.metrics.reporters.statsd.StatsDErrorReporter;
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

import static com.google.api.client.util.Preconditions.checkArgument;
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
    private final StatsDErrorReporter statsDErrorReporter;

    private ChronologyOrderedSplitAssigner(Collection<FileSourceSplit> fileSourceSplits, PathParser pathParser,
                                           TimeRangePool timeRangePool, SerializedStatsDReporterSupplier statsDReporterSupplier) {
        this.pathParser = pathParser;
        this.timeRangePool = timeRangePool;
        this.statsDErrorReporter = new StatsDErrorReporter(statsDReporterSupplier);
        this.unassignedSplits = new PriorityBlockingQueue<>(INITIAL_DEFAULT_CAPACITY, getFileSourceSplitComparator());
        this.daggerGaugeManager = new DaggerGaugeManager(statsDReporterSupplier);
        initAndValidate(fileSourceSplits);
    }

    private void initAndValidate(Collection<FileSourceSplit> fileSourceSplits) {
        StatsDTag[] splitAssignerTags = getSplitAssignerTags();
        daggerGaugeManager.register(splitAssignerTags);
        daggerGaugeManager.markValue(TOTAL_SPLITS_DISCOVERED, fileSourceSplits.size());
        for (FileSourceSplit split : fileSourceSplits) {
            validateAndAddSplits(split);
        }
        daggerGaugeManager.markValue(TOTAL_SPLITS_RECORDED, unassignedSplits.size());
        daggerGaugeManager.markValue(SPLITS_AWAITING_ASSIGNMENT, unassignedSplits.size());
    }

    @Override
    public Optional<FileSourceSplit> getNext(@Nullable String hostname) {
        InstantEnrichedSplit instantEnrichedSplit = unassignedSplits.poll();
        if (instantEnrichedSplit == null) {
            return Optional.empty();
        }
        daggerGaugeManager.markValue(SPLITS_AWAITING_ASSIGNMENT, unassignedSplits.size());
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
            IllegalArgumentException exception = new IllegalArgumentException(ex);
            statsDErrorReporter.reportFatalException(exception);
            throw exception;
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
        private SerializedStatsDReporterSupplier statsDReporterSupplier;

        public ChronologyOrderedSplitAssignerBuilder addPathParser(PathParser parser) {
            this.pathParser = parser;
            return this;
        }

        public ChronologyOrderedSplitAssignerBuilder addTimeRanges(TimeRangePool timeRangePool) {
            this.parquetFileDateRange = timeRangePool;
            return this;
        }

        public ChronologyOrderedSplitAssignerBuilder addStatsDReporterSupplier(SerializedStatsDReporterSupplier supplier) {
            this.statsDReporterSupplier = supplier;
            return this;
        }

        public ChronologyOrderedSplitAssigner build(Collection<FileSourceSplit> fileSourceSplits) {
            checkArgument(statsDReporterSupplier != null, "SerializedStatsDReporterSupplier is required but is set as null");
            if (pathParser == null) {
                PathParserNotProvidedException exception = new PathParserNotProvidedException("Path parser is null");
                new StatsDErrorReporter(statsDReporterSupplier).reportFatalException(exception);
                throw exception;
            }
            return new ChronologyOrderedSplitAssigner(fileSourceSplits, pathParser, parquetFileDateRange, statsDReporterSupplier);
        }
    }
}
