package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.serde.parquet.parser.validation.SimpleGroupValidation;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.sql.Timestamp;
import java.time.Instant;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ParquetTimestampParserTest {
    @Mock
    SimpleGroupValidation simpleGroupValidation;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void canHandleReturnsFalseIfValidationChecksFail() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(false);
        ParquetTimestampParser timestampParser = new ParquetTimestampParser(simpleGroupValidation);

        boolean canHandle = timestampParser.canHandle(null, "column-with-timestamp");

        assertFalse(canHandle);
    }

    @Test
    public void getParserReturnsTrueIfValidationChecksPass() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("sample-timestamp-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("sample-timestamp-column", Long.MIN_VALUE);

        ParquetBooleanParser parquetBooleanParser = new ParquetBooleanParser(simpleGroupValidation);

        boolean canHandle = parquetBooleanParser.canHandle(simpleGroup, "sample-timestamp-column");

        assertTrue(canHandle);
    }

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

        ParquetTimestampParser timestampParser = new ParquetTimestampParser(simpleGroupValidation);
        Object actualLargestTimestamp = timestampParser.deserialize(simpleGroup, "column-with-max-timestamp-in-millis");
        Object actualSmallestTimestamp = timestampParser.deserialize(simpleGroup, "column-with-min-timestamp-in-millis");

        assertEquals(largestPossibleTimestamp, actualLargestTimestamp);
        assertEquals(expectedSmallestTimestamp, actualSmallestTimestamp);
    }

    @Test
    public void deserializeShouldReturnUTCEpochTimestampWhenDeserializedValueIsNull() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("column-with-timestamp-in-millis")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        Timestamp epochTimestamp = Timestamp.from(Instant.EPOCH);
        ParquetTimestampParser parquetTimestampParser = new ParquetTimestampParser(simpleGroupValidation);

        Object actualTimestamp = parquetTimestampParser.deserialize(simpleGroup, "column-with-timestamp-in-millis");

        assertEquals(epochTimestamp, actualTimestamp);
    }
}