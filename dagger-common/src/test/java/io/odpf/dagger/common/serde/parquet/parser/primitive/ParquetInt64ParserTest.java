package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.serde.parquet.parser.validation.SimpleGroupValidation;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ParquetInt64ParserTest {
    @Mock
    SimpleGroupValidation simpleGroupValidation;

    @Before
    public void setup() {
        initMocks(this);
    }


    @Test
    public void canHandleReturnsFalseIfValidationChecksFail() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(false);
        ParquetInt64Parser parquetInt64Parser = new ParquetInt64Parser(simpleGroupValidation);

        boolean canHandle = parquetInt64Parser.canHandle(null, "column-with-int64-value");

        assertFalse(canHandle);
    }

    @Test
    public void getParserReturnsTrueIfValidationChecksPass() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64).named("int64-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ParquetInt64Parser parquetInt64Parser = new ParquetInt64Parser(simpleGroupValidation);

        boolean canHandle = parquetInt64Parser.canHandle(simpleGroup, "int64-column");

        assertTrue(canHandle);
    }

    @Test
    public void deserializeShouldParseParquetInt64ToJavaLongType() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64).named("column-with-max-long-value")
                .required(INT64).named("column-with-min-long-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("column-with-max-long-value", Long.MAX_VALUE);
        simpleGroup.add("column-with-min-long-value", Long.MIN_VALUE);

        ParquetInt64Parser int64Parser = new ParquetInt64Parser(simpleGroupValidation);
        Object actualValueForMaxLongColumn = int64Parser.deserialize(simpleGroup, "column-with-max-long-value");
        Object actualValueForMinLongColumn = int64Parser.deserialize(simpleGroup, "column-with-min-long-value");

        assertEquals(Long.MAX_VALUE, actualValueForMaxLongColumn);
        assertEquals(Long.MIN_VALUE, actualValueForMinLongColumn);
    }

    @Test
    public void deserializeShouldReturnDefaultValueWhenDeserializedValueIsNull() {
        /* The field is added to the schema but not assigned a value */
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64).named("some-random-field")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        ParquetInt64Parser parquetInt64Parser = new ParquetInt64Parser(simpleGroupValidation);

        Object actualValue = parquetInt64Parser.deserialize(simpleGroup, "some-random-field");

        assertEquals(0L, actualValue);
    }
}