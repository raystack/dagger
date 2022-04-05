package io.odpf.dagger.core.source.parquet.splitassigner;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ChronologyOrderedSplitAssigner implements FileSplitAssigner {
    private final PriorityBlockingQueue<InstantEnrichedSplit> unassignedSplits;
    private final Pattern filePathPattern;
    private static final int INITIAL_DEFAULT_CAPACITY = 11;

    private ChronologyOrderedSplitAssigner(Collection<FileSourceSplit> fileSourceSplits, String chronologicalFilePathRegex) {
        this.unassignedSplits = new PriorityBlockingQueue<>(INITIAL_DEFAULT_CAPACITY, getFileSourceSplitComparator());
        this.filePathPattern = Pattern.compile(chronologicalFilePathRegex);
        for (FileSourceSplit split : fileSourceSplits) {
            validateAndAddSplits(split);
        }
    }

    private void validateAndAddSplits(FileSourceSplit split) {
        try {
            Instant instant = parseInstantFromFilePath(split.path());
            this.unassignedSplits.add(new InstantEnrichedSplit(split, instant));
        } catch (ParseException ex) {
            throw new IllegalArgumentException(ex);
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

    private Instant parseInstantFromFilePath(Path path) throws ParseException {
        Matcher matcher = filePathPattern.matcher(path.toString());
        final int hourMatcherGroupNumber = 3;
        final int dateMatcherGroupNumber = 1;
        boolean matchFound = matcher.find();
        if (matchFound && matcher.group(hourMatcherGroupNumber) != null && matcher.group(dateMatcherGroupNumber) != null) {
            return convertToInstant(matcher.group(dateMatcherGroupNumber), matcher.group(hourMatcherGroupNumber));
        } else if (matchFound && matcher.group(hourMatcherGroupNumber) == null && matcher.group(dateMatcherGroupNumber) != null) {
            return convertToInstant(matcher.group(dateMatcherGroupNumber));
        } else {
            String message = String.format("Cannot extract timestamp from filepath for deciding order of processing.\n"
                    + "File path %s doesn't match with configured SOURCE_PARQUET_CHRONOLOGICAL_FILE_PATH_REGEX of %s", path, filePathPattern.pattern());
            throw new ParseException(message, 0);
        }
    }

    private Instant convertToInstant(String dateSegment) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        return formatter.parse(dateSegment).toInstant();
    }

    private Instant convertToInstant(String dateSegment, String hourSegment) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH");
        String dateHourString = String.join(" ", dateSegment, hourSegment);
        return formatter.parse(dateHourString).toInstant();
    }

    public static class ChronologyOrderedSplitAssignerProvider implements FileSplitAssigner.Provider {
        private final String chronologicalFilePathRegex;

        public ChronologyOrderedSplitAssignerProvider(String chronologicalFilePathRegex) {
            this.chronologicalFilePathRegex = chronologicalFilePathRegex;
        }

        @Override
        public FileSplitAssigner create(Collection<FileSourceSplit> initialSplits) {
            return new ChronologyOrderedSplitAssigner(initialSplits, chronologicalFilePathRegex);
        }
    }
}
