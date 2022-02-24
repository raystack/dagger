package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.exceptions.serde.DaggerDeserializationException;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.junit.Assert.*;

public class ParquetTimestampParserTest {

    @Test
    public void deserializeShouldParseParquetInt64WithLogicalTypeAnnotationAsTimestampToJavaTimestamp() {
        /* Parquet stores timestamps as int64, which maps to a long data type in Java */
        long largestPossibleInstantInMillis = Long.MAX_VALUE;
        Timestamp largestPossibleTimestamp = Timestamp.from(Instant.ofEpochMilli(largestPossibleInstantInMillis));
        long smallestPossibleInstantInMillis = Long.MIN_VALUE;
        Timestamp expectedSmallestTimestamp = Timestamp.from(Instant.ofEpochMilli(smallestPossibleInstantInMillis));
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("column-with-max-timestamp-in-millis")
                .required(INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("column-with-min-timestamp-in-millis")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("column-with-max-timestamp-in-millis", largestPossibleInstantInMillis);
        simpleGroup.add("column-with-min-timestamp-in-millis", smallestPossibleInstantInMillis);

        ParquetTimestampParser timestampParser = new ParquetTimestampParser();
        Object actualLargestTimestamp = timestampParser.deserialize(simpleGroup, "column-with-max-timestamp-in-millis");
        Object actualSmallestTimestamp = timestampParser.deserialize(simpleGroup, "column-with-min-timestamp-in-millis");

        assertEquals(largestPossibleTimestamp, actualLargestTimestamp);
        assertEquals(expectedSmallestTimestamp, actualSmallestTimestamp);
    }

    @Test(expected = DaggerDeserializationException.class)
    public void deserializeShouldThrowExceptionWhenLogicalTypeAnnotationIsMissing() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64)
                .named("sample-int64-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("sample-int64-column", Long.MIN_VALUE);

        ParquetTimestampParser parquetTimestampParser = new ParquetTimestampParser();
        parquetTimestampParser.deserialize(simpleGroup, "sample-int64-column");
    }

    @Test(expected = DaggerDeserializationException.class)
    public void deserializeShouldThrowExceptionWhenLogicalTypeAnnotationIsNotSupported() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                .named("sample-int64-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("sample-int64-column", Long.MIN_VALUE);

        ParquetTimestampParser parquetTimestampParser = new ParquetTimestampParser();
        parquetTimestampParser.deserialize(simpleGroup, "sample-int64-column");
    }

    @Test
    public void deserializeShouldReturnUTCEpochTimestampWhenSimpleGroupArgumentIsNull() {
        ParquetTimestampParser parquetTimestampParser = new ParquetTimestampParser();
        Timestamp epochTimestamp = Timestamp.from(Instant.EPOCH);

        Object actualTimestamp = parquetTimestampParser.deserialize(null, "some-random-field");

        assertEquals(epochTimestamp, actualTimestamp);
    }
}