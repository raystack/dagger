package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.serde.parquet.parser.validation.SimpleGroupValidation;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ParquetDoubleParserTest {
    @Mock
    SimpleGroupValidation simpleGroupValidation;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void canHandleReturnsFalseIfValidationChecksFail() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(false);
        ParquetDoubleParser parquetDoubleParser = new ParquetDoubleParser(simpleGroupValidation);

        boolean canHandle = parquetDoubleParser.canHandle(null, "column-with-boolean-true-value");

        assertFalse(canHandle);
    }

    @Test
    public void getParserReturnsTrueIfValidationChecksPass() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);
        GroupType parquetSchema = Types.requiredGroup()
                .required(DOUBLE).named("column-with-double-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;
        simpleGroup.add("column-with-double-value", Double.MIN_VALUE);
        ParquetDoubleParser doubleParser = new ParquetDoubleParser(simpleGroupValidation);

        boolean canHandle = doubleParser.canHandle(simpleGroup, "column-with-double-value");

        assertTrue(canHandle);
    }


    @Test
    public void deserializeShouldParseParquetDoubleToJavaDoubleType() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(DOUBLE).named("column-with-max-double-value")
                .required(DOUBLE).named("column-with-min-double-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;
        simpleGroup.add("column-with-max-double-value", Double.MAX_VALUE);
        simpleGroup.add("column-with-min-double-value", Double.MIN_VALUE);

        ParquetDoubleParser doubleParser = new ParquetDoubleParser(simpleGroupValidation);
        Object actualValueForMaxDoubleColumn = doubleParser.deserialize(simpleGroup, "column-with-max-double-value");
        Object actualValueForMinDoubleColumn = doubleParser.deserialize(simpleGroup, "column-with-min-double-value");

        assertEquals(Double.MAX_VALUE, actualValueForMaxDoubleColumn);
        assertEquals(Double.MIN_VALUE, actualValueForMinDoubleColumn);
    }

    @Test
    public void deserializeShouldReturnDefaultValueWhenDeserializedValueIsNull() {
        /* The field is added to the schema but not assigned a value */
        GroupType parquetSchema = Types.requiredGroup()
                .required(DOUBLE).named("some-random-field")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;
        ParquetDoubleParser parquetDoubleParser = new ParquetDoubleParser(simpleGroupValidation);
        Object actualValue = parquetDoubleParser.deserialize(simpleGroup, "some-random-field");

        assertEquals(0.0d, actualValue);
    }
}