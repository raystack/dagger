package io.odpf.dagger.core.source.parquet.splitassigner;


import io.odpf.dagger.core.source.parquet.splitassigner.ChronologyOrderedSplitAssigner.ChronologyOrderedSplitAssignerProvider;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class ChronologyOrderedSplitAssignerTest {
    private final String parquetChronologicalFilePathRegex = "^.*/dt=([0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9])/(hr=([0-9][0-9]))?.*$";

    @Test
    public void shouldReturnFileSplitsHavingOldestDateFilePathsFirstWhenFilePathURLHasOnlyDate() {
        FileSourceSplit firstSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-10-12/asdghsdhasd"), 0, 1024);
        FileSourceSplit secondSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-02-29/ga6agad6ad"), 0, 1024);
        FileSourceSplit thirdSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-11-31/hd7ahadh7agd"), 0, 1024);
        FileSourceSplit fourthSplit = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2020-12-31/hagga6a36dg"), 0, 1024);
        List<FileSourceSplit> inputSplits = Arrays.asList(secondSplit, fourthSplit, firstSplit, thirdSplit);
        FileSourceSplit[] expectedOrdering = new FileSourceSplit[]{firstSplit, secondSplit, thirdSplit, fourthSplit};

        ChronologyOrderedSplitAssignerProvider chronologyOrderedSplitAssignerProvider = new ChronologyOrderedSplitAssignerProvider(parquetChronologicalFilePathRegex);
        ChronologyOrderedSplitAssigner splitAssigner = (ChronologyOrderedSplitAssigner) chronologyOrderedSplitAssignerProvider.create(inputSplits);

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

        ChronologyOrderedSplitAssignerProvider chronologyOrderedSplitAssignerProvider = new ChronologyOrderedSplitAssignerProvider(parquetChronologicalFilePathRegex);
        ChronologyOrderedSplitAssigner splitAssigner = (ChronologyOrderedSplitAssigner) chronologyOrderedSplitAssignerProvider.create(inputSplits);

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

        ChronologyOrderedSplitAssignerProvider chronologyOrderedSplitAssignerProvider = new ChronologyOrderedSplitAssignerProvider(parquetChronologicalFilePathRegex);
        ChronologyOrderedSplitAssigner splitAssigner = (ChronologyOrderedSplitAssigner) chronologyOrderedSplitAssignerProvider.create(inputSplits);

        splitAssigner.getNext(null);
        Optional<FileSourceSplit> nextSplit = splitAssigner.getNext(null);

        assertFalse(nextSplit.isPresent());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionDuringConstructionItselfWhenFilePathsDoNotFollowPattern() {
        FileSourceSplit split = new FileSourceSplit("1", new Path("gs://my-bucket/bid-log/dt=2019-130-12/shs6s5sdg"), 0, 1024);

        IllegalArgumentException ex = Assert.assertThrows(IllegalArgumentException.class, () -> {
            ChronologyOrderedSplitAssignerProvider chronologyOrderedSplitAssignerProvider = new ChronologyOrderedSplitAssignerProvider(parquetChronologicalFilePathRegex);
            chronologyOrderedSplitAssignerProvider.create(Collections.singleton(split));
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
        ChronologyOrderedSplitAssignerProvider chronologyOrderedSplitAssignerProvider = new ChronologyOrderedSplitAssignerProvider(parquetChronologicalFilePathRegex);
        ChronologyOrderedSplitAssigner splitAssigner = (ChronologyOrderedSplitAssigner) chronologyOrderedSplitAssignerProvider.create(Collections.singleton(secondSplit));

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
        ChronologyOrderedSplitAssignerProvider chronologyOrderedSplitAssignerProvider = new ChronologyOrderedSplitAssignerProvider(parquetChronologicalFilePathRegex);
        ChronologyOrderedSplitAssigner splitAssigner = (ChronologyOrderedSplitAssigner) chronologyOrderedSplitAssignerProvider.create(Collections.singleton(secondSplit));

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

        ChronologyOrderedSplitAssignerProvider chronologyOrderedSplitAssignerProvider = new ChronologyOrderedSplitAssignerProvider(parquetChronologicalFilePathRegex);
        ChronologyOrderedSplitAssigner splitAssigner = (ChronologyOrderedSplitAssigner) chronologyOrderedSplitAssignerProvider.create(inputSplits);

        splitAssigner.getNext(null);
        List<FileSourceSplit> remainingSplits = (List<FileSourceSplit>) splitAssigner.remainingSplits();

        FileSourceSplit[] expectedSplits = new FileSourceSplit[]{secondSplit, thirdSplit, fourthSplit};
        for (int i = 0; i < 3; i++) {
            assertEquals("AssertionError when testing for file split number " + (i + 1), expectedSplits[i], remainingSplits.get(i));
        }
    }
}
