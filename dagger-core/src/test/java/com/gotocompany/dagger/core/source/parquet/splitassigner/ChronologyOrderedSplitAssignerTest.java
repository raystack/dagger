package com.gotocompany.dagger.core.source.parquet.splitassigner;


import com.gotocompany.dagger.core.exception.PathParserNotProvidedException;
import com.gotocompany.dagger.core.metrics.aspects.ChronologyOrderedSplitAssignerAspects;
import com.gotocompany.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import com.gotocompany.dagger.core.source.config.models.TimeRange;
import com.gotocompany.dagger.core.source.config.models.TimeRangePool;
import com.gotocompany.dagger.core.source.parquet.path.HourDatePathParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.text.ParseException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class ChronologyOrderedSplitAssignerTest {

    @Mock
    private StatsDReporter statsDReporter;

    private final SerializedStatsDReporterSupplier statsDReporterSupplierMock = () -> statsDReporter;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnFileSplitsHavingOldestDateFilePathsFirstWhenFilePathURLHasOnlyDate() {
        FileSourceSplit firstSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/asdghsdhasd"), 0, 1024);
        FileSourceSplit secondSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-02-29/ga6agad6ad"), 0, 1024);
        FileSourceSplit thirdSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-11-31/hd7ahadh7agd"), 0, 1024);
        FileSourceSplit fourthSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-12-31/hagga6a36dg"), 0, 1024);
        List<FileSourceSplit> inputSplits = Arrays.asList(secondSplit, fourthSplit, firstSplit, thirdSplit);
        FileSourceSplit[] expectedOrdering = new FileSourceSplit[]{firstSplit, secondSplit, thirdSplit, fourthSplit};

        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();

        ChronologyOrderedSplitAssigner splitAssigner = splitAssignerBuilder.addPathParser(new HourDatePathParser())
                .addStatsDReporterSupplier(statsDReporterSupplierMock)
                .build(inputSplits);

        for (int i = 0; i < 4; i++) {
            Optional<FileSourceSplit> split = splitAssigner.getNext(null);
            assertTrue(split.isPresent());
            assertEquals("AssertionError when testing for file split number " + (i + 1), expectedOrdering[i], split.get());
        }
    }

    @Test
    public void shouldReturnFileSplitsHavingOldestTimeFilePathsFirstWhenFilePathURLHasBothDateAndHour() {
        FileSourceSplit firstSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/hr=00/hd6a7gad"), 0, 1024);
        FileSourceSplit secondSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/hr=08/sa6advgad7"), 0, 1024);
        FileSourceSplit thirdSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-11-31/hr=09/aga6adgad"), 0, 1024);
        FileSourceSplit fourthSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-12-31/hr=23/ahaha4a5dg"), 0, 1024);
        List<FileSourceSplit> inputSplits = Arrays.asList(secondSplit, fourthSplit, firstSplit, thirdSplit);
        FileSourceSplit[] expectedOrdering = new FileSourceSplit[]{firstSplit, secondSplit, thirdSplit, fourthSplit};

        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();

        ChronologyOrderedSplitAssigner splitAssigner = splitAssignerBuilder.addPathParser(new HourDatePathParser())
                .addStatsDReporterSupplier(statsDReporterSupplierMock)
                .build(inputSplits);

        for (int i = 0; i < 4; i++) {
            Optional<FileSourceSplit> split = splitAssigner.getNext(null);
            assertTrue(split.isPresent());
            assertEquals("AssertionError when testing for file split number " + (i + 1), expectedOrdering[i], split.get());
        }
    }

    @Test
    public void shouldReturnEmptyOptionalWhenNoMoreSplitsToReturn() {
        FileSourceSplit firstSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/shs6s5sdg"), 0, 1024);
        List<FileSourceSplit> inputSplits = Arrays.asList(firstSplit);

        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();

        ChronologyOrderedSplitAssigner splitAssigner = splitAssignerBuilder.
                addPathParser(new HourDatePathParser())
                .addStatsDReporterSupplier(statsDReporterSupplierMock)
                .build(inputSplits);

        splitAssigner.getNext(null);
        Optional<FileSourceSplit> nextSplit = splitAssigner.getNext(null);

        assertFalse(nextSplit.isPresent());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionDuringConstructionItselfWhenFilePathsDoNotFollowPattern() {
        FileSourceSplit split = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-130-12/shs6s5sdg"), 0, 1024);

        IllegalArgumentException ex = Assert.assertThrows(IllegalArgumentException.class, () -> {
            ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();

            splitAssignerBuilder
                    .addPathParser(new HourDatePathParser())
                    .addStatsDReporterSupplier(statsDReporterSupplierMock)
                    .build(Collections.singleton(split));
        });

        assertEquals("java.text.ParseException: Cannot extract timestamp from filepath for deciding order of processing.\n"
                + "File path doesn't abide with any partitioning strategy: gs://my-bucket/bid-log/dt=2019-130-12/shs6s5sdg", ex.getMessage());
    }

    @Test
    public void shouldAddNewFileSourceSplitsWithOldestDateFilePathsReturnedFirstWhenFilePathURLHasOnlyDate() {
        FileSourceSplit firstSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/asdghsdhasd"), 0, 1024);
        FileSourceSplit secondSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-02-29/ga6agad6ad"), 0, 1024);
        FileSourceSplit thirdSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-11-31/hd7ahadh7agd"), 0, 1024);
        FileSourceSplit fourthSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-12-31/hagga6a36dg"), 0, 1024);
        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();

        ChronologyOrderedSplitAssigner splitAssigner = splitAssignerBuilder
                .addPathParser(new HourDatePathParser())
                .addStatsDReporterSupplier(statsDReporterSupplierMock)
                .build(Collections.singleton(secondSplit));
        List<FileSourceSplit> remainingSplitsToAdd = Arrays.asList(fourthSplit, firstSplit, thirdSplit);
        splitAssigner.addSplits(remainingSplitsToAdd);

        FileSourceSplit[] expectedOrdering = new FileSourceSplit[]{firstSplit, secondSplit, thirdSplit, fourthSplit};
        for (int i = 0; i < 4; i++) {
            Optional<FileSourceSplit> split = splitAssigner.getNext(null);
            assertTrue(split.isPresent());
            assertEquals("AssertionError when testing for file split number " + (i + 1), expectedOrdering[i], split.get());
        }
    }

    @Test
    public void shouldAddNewFileSourceSplitsWithOldestTimeFilePathsReturnedFirstWhenFilePathURLHasBothDateAndHour() {
        FileSourceSplit firstSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/hr=00/hd6a7gad"), 0, 1024);
        FileSourceSplit secondSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/hr=08/sa6advgad7"), 0, 1024);
        FileSourceSplit thirdSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-11-31/hr=09/aga6adgad"), 0, 1024);
        FileSourceSplit fourthSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-12-31/hr=23/ahaha4a5dg"), 0, 1024);
        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();

        ChronologyOrderedSplitAssigner splitAssigner = splitAssignerBuilder
                .addPathParser(new HourDatePathParser())
                .addStatsDReporterSupplier(statsDReporterSupplierMock)
                .build(Collections.singleton(secondSplit));
        List<FileSourceSplit> remainingSplitsToAdd = Arrays.asList(fourthSplit, firstSplit, thirdSplit);
        splitAssigner.addSplits(remainingSplitsToAdd);

        FileSourceSplit[] expectedOrdering = new FileSourceSplit[]{firstSplit, secondSplit, thirdSplit, fourthSplit};
        for (int i = 0; i < 4; i++) {
            Optional<FileSourceSplit> split = splitAssigner.getNext(null);
            assertTrue(split.isPresent());
            assertEquals("AssertionError when testing for file split number " + (i + 1), expectedOrdering[i], split.get());
        }
    }

    @Test
    public void shouldReturnRemainingSplitsWhichAreNotAssignedYetInAscendingOrderOfFilePathTimestampURL() {
        FileSourceSplit firstSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/asdghsdhasd"), 0, 1024);
        FileSourceSplit secondSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-02-29/ga6agad6ad"), 0, 1024);
        FileSourceSplit thirdSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-11-31/hd7ahadh7agd"), 0, 1024);
        FileSourceSplit fourthSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-12-31/hagga6a36dg"), 0, 1024);
        List<FileSourceSplit> inputSplits = Arrays.asList(secondSplit, fourthSplit, firstSplit, thirdSplit);

        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();

        ChronologyOrderedSplitAssigner splitAssigner = splitAssignerBuilder
                .addPathParser(new HourDatePathParser())
                .addStatsDReporterSupplier(statsDReporterSupplierMock)
                .build(inputSplits);
        splitAssigner.getNext(null);
        List<FileSourceSplit> remainingSplits = (List<FileSourceSplit>) splitAssigner.remainingSplits();

        FileSourceSplit[] expectedSplits = new FileSourceSplit[]{secondSplit, thirdSplit, fourthSplit};
        for (int i = 0; i < 3; i++) {
            assertEquals("AssertionError when testing for file split number " + (i + 1), expectedSplits[i], remainingSplits.get(i));
        }
    }

    @Test
    public void shouldCallPathParserToParseFilePathURL() throws ParseException {
        HourDatePathParser hourDatePathParser = Mockito.mock(HourDatePathParser.class);
        FileSourceSplit split = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/asdghsdhasd"), 0, 1024);

        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();

        splitAssignerBuilder
                .addPathParser(hourDatePathParser)
                .addStatsDReporterSupplier(statsDReporterSupplierMock)
                .build(Collections.singleton(split));

        verify(hourDatePathParser, times(1)).instantFromFilePath(split.path());
    }

    @Test
    public void shouldReturnFileSplitsFallingInGivenTimeRangeWhenFilePathURLHasBothDateAndHour() {
        FileSourceSplit firstSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/hr=00/hd6a7gad"), 0, 1024);
        FileSourceSplit secondSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/hr=08/sa6advgad7"), 0, 1024);
        FileSourceSplit thirdSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-11-30/hr=09/aga6adgad"), 0, 1024);
        FileSourceSplit fourthSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-12-31/hr=23/ahaha4a5dg"), 0, 1024);
        List<FileSourceSplit> inputSplits = Arrays.asList(secondSplit, fourthSplit, firstSplit, thirdSplit);

        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();

        TimeRangePool timeRangePool = new TimeRangePool();
        timeRangePool.add(new TimeRange(Instant.parse("2019-10-12T00:00:00Z"), Instant.parse("2020-11-30T10:00:00Z")));
        ChronologyOrderedSplitAssigner splitAssigner = splitAssignerBuilder
                .addPathParser(new HourDatePathParser())
                .addTimeRanges(timeRangePool)
                .addStatsDReporterSupplier(statsDReporterSupplierMock)
                .build(inputSplits);

        assertEquals(firstSplit, splitAssigner.getNext(null).get());
        assertEquals(secondSplit, splitAssigner.getNext(null).get());
        assertEquals(thirdSplit, splitAssigner.getNext(null).get());
        assertFalse(splitAssigner.getNext(null).isPresent());
    }

    @Test
    public void shouldReturnFileSplitsFallingInMultipleTimeRangesWhenFilePathURLHasBothDateAndHour() {
        FileSourceSplit firstSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/hr=00/hd6a7gad"), 0, 1024);
        FileSourceSplit secondSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/hr=08/sa6advgad7"), 0, 1024);
        FileSourceSplit thirdSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-11-30/hr=09/aga6adgad"), 0, 1024);
        FileSourceSplit fourthSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-12-31/hr=23/ahaha4a5dg"), 0, 1024);
        List<FileSourceSplit> inputSplits = Arrays.asList(secondSplit, fourthSplit, firstSplit, thirdSplit);

        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();

        TimeRangePool timeRangePool = new TimeRangePool();
        timeRangePool.add(new TimeRange(Instant.parse("2019-10-12T00:00:00Z"), Instant.parse("2019-10-12T04:00:00Z")));
        timeRangePool.add(new TimeRange(Instant.parse("2020-11-29T00:00:00Z"), Instant.parse("2020-11-30T10:00:00Z")));
        ChronologyOrderedSplitAssigner splitAssigner = splitAssignerBuilder
                .addPathParser(new HourDatePathParser())
                .addTimeRanges(timeRangePool)
                .addStatsDReporterSupplier(statsDReporterSupplierMock)
                .build(inputSplits);

        assertEquals(firstSplit, splitAssigner.getNext(null).get());
        assertEquals(thirdSplit, splitAssigner.getNext(null).get());
        assertFalse(splitAssigner.getNext(null).isPresent());
    }

    @Test
    public void shouldThrowExceptionIfPathParserNotProvided() {
        FileSourceSplit firstSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/hr=00/hd6a7gad"), 0, 1024);
        FileSourceSplit secondSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/hr=08/sa6advgad7"), 0, 1024);
        FileSourceSplit thirdSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-11-30/hr=09/aga6adgad"), 0, 1024);
        FileSourceSplit fourthSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-12-31/hr=23/ahaha4a5dg"), 0, 1024);
        List<FileSourceSplit> inputSplits = Arrays.asList(secondSplit, fourthSplit, firstSplit, thirdSplit);

        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();

        TimeRangePool timeRangePool = new TimeRangePool();
        timeRangePool.add(new TimeRange(Instant.parse("2019-10-12T00:00:00Z"), Instant.parse("2019-10-12T04:00:00Z")));
        timeRangePool.add(new TimeRange(Instant.parse("2020-11-29T00:00:00Z"), Instant.parse("2020-11-30T10:00:00Z")));
        PathParserNotProvidedException pathParserNotProvidedException = assertThrows(PathParserNotProvidedException.class,
                () -> splitAssignerBuilder
                        .addTimeRanges(timeRangePool)
                        .addStatsDReporterSupplier(statsDReporterSupplierMock)
                        .build(inputSplits));
        assertEquals("Path parser is null", pathParserNotProvidedException.getMessage());
    }

    @Test
    public void shouldRaiseMetricsForDiscoveredSplitsAndRecordedSplitsWithTagsWhenInitialized() {
        FileSourceSplit firstSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/hr=00/hd6a7gad"), 0, 1024);
        FileSourceSplit secondSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/hr=08/sa6advgad7"), 0, 1024);
        FileSourceSplit thirdSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-11-30/hr=09/aga6adgad"), 0, 1024);
        FileSourceSplit fourthSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-12-31/hr=23/ahaha4a5dg"), 0, 1024);
        List<FileSourceSplit> inputSplits = Arrays.asList(secondSplit, fourthSplit, firstSplit, thirdSplit);

        TimeRangePool timeRangePool = new TimeRangePool();
        timeRangePool.add(new TimeRange(Instant.parse("2019-10-12T00:00:00Z"), Instant.parse("2020-11-30T10:00:00Z")));

        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();

        splitAssignerBuilder
                .addPathParser(new HourDatePathParser())
                .addTimeRanges(timeRangePool)
                .addStatsDReporterSupplier(statsDReporterSupplierMock)
                .build(inputSplits);

        verify(statsDReporter, times(1)).gauge(ChronologyOrderedSplitAssignerAspects.TOTAL_SPLITS_DISCOVERED.getValue(), 4, "component=split_assigner");
        verify(statsDReporter, times(1)).gauge(ChronologyOrderedSplitAssignerAspects.TOTAL_SPLITS_RECORDED.getValue(), 3, "component=split_assigner");
    }

    @Test
    public void shouldRaiseMetricsWithTagsAfterAssigningSplits() {
        FileSourceSplit firstSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/asdghsdhasd"), 0, 1024);
        FileSourceSplit secondSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-02-29/ga6agad6ad"), 0, 1024);
        List<FileSourceSplit> inputSplits = Arrays.asList(firstSplit, secondSplit);

        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();
        ChronologyOrderedSplitAssigner splitAssigner = splitAssignerBuilder.addPathParser(new HourDatePathParser())
                .addStatsDReporterSupplier(statsDReporterSupplierMock)
                .build(inputSplits);
        splitAssigner.getNext(null);

        verify(statsDReporter, times(1)).gauge(ChronologyOrderedSplitAssignerAspects.SPLITS_AWAITING_ASSIGNMENT.getValue(), 1, "component=split_assigner");
    }

    @Test
    public void shouldRaiseErrorMetricsWhenFilePathValidationFailed() {
        FileSourceSplit split = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-130-12/shs6s5sdg"), 0, 1024);
        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();
        assertThrows(IllegalArgumentException.class, () -> splitAssignerBuilder
                .addPathParser(new HourDatePathParser())
                .addStatsDReporterSupplier(statsDReporterSupplierMock)
                .build(Collections.singleton(split)));

        verify(statsDReporter, times(1))
                .captureCount("fatal.exception", 1L, "fatal_exception_type=" + IllegalArgumentException.class.getName());
    }

    @Test
    public void shouldThrowExceptionIfStatsDSupplierNotProvided() {
        ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder splitAssignerBuilder = new ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerBuilder();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> splitAssignerBuilder
                        .addPathParser(new HourDatePathParser())
                        .build(Collections.emptyList()));

        assertEquals("SerializedStatsDReporterSupplier is required but is set as null", exception.getMessage());
    }
}
