package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.serde.parquet.parser.validation.SimpleGroupValidation;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ParquetFloatParserTest {

    @Mock
    SimpleGroupValidation simpleGroupValidation;

    @Before
    public void setup() {
        initMocks(this);
    }


    @Test
    public void canHandleReturnsFalseIfValidationChecksFail() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(false);
        ParquetFloatParser parquetFloatParser = new ParquetFloatParser(simpleGroupValidation);

        boolean canHandle = parquetFloatParser.canHandle(null, "column-with-float-value");

        assertFalse(canHandle);
    }

    @Test
    public void getParserReturnsTrueIfValidationChecksPass() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);
        GroupType parquetSchema = Types.requiredGroup()
                .required(FLOAT).named("column-with-float-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        ParquetFloatParser parquetFloatParser = new ParquetFloatParser(simpleGroupValidation);

        boolean canHandle = parquetFloatParser.canHandle(simpleGroup, "column-with-float-value");

        assertTrue(canHandle);
    }


    @Test
    public void deserializeShouldParseParquetFloatToJavaFloatType() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(FLOAT).named("column-with-max-float-value")
                .required(FLOAT).named("column-with-min-float-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;
        simpleGroup.add("column-with-max-float-value", Float.MAX_VALUE);
        simpleGroup.add("column-with-min-float-value", Float.MIN_VALUE);

        ParquetFloatParser floatParser = new ParquetFloatParser(simpleGroupValidation);
        Object actualValueForMaxFloatColumn = floatParser.deserialize(simpleGroup, "column-with-max-float-value");
        Object actualValueForMinFloatColumn = floatParser.deserialize(simpleGroup, "column-with-min-float-value");

        assertEquals(Float.MAX_VALUE, actualValueForMaxFloatColumn);
        assertEquals(Float.MIN_VALUE, actualValueForMinFloatColumn);
    }

    @Test
    public void deserializeShouldReturnDefaultValueWhenDeserializedValueIsNull() {
        /* The field is added to the schema but not assigned a value */
        GroupType parquetSchema = Types.requiredGroup()
                .required(FLOAT).named("some-random-field")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;
        ParquetFloatParser parquetFloatParser = new ParquetFloatParser(simpleGroupValidation);

        Object actualValue = parquetFloatParser.deserialize(simpleGroup, "some-random-field");

        assertEquals(0.0f, actualValue);
    }
}