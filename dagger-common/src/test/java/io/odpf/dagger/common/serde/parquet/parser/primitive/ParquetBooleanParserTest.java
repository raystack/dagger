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

public class ParquetBooleanParserTest {

    @Mock
    SimpleGroupValidation simpleGroupValidation;

    @Before
    public void setup() {
        initMocks(this);
    }


    @Test
    public void canHandleReturnsFalseIfValidationChecksFail() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(false);
        GroupType parquetSchema = Types.requiredGroup()
                .required(BOOLEAN)
                .named("column-with-boolean-true-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ParquetBooleanParser parquetBooleanParser = new ParquetBooleanParser(simpleGroupValidation);

        boolean canHandle = parquetBooleanParser.canHandle(simpleGroup, "column-with-boolean-true-value");

        assertFalse(canHandle);
    }

    @Test
    public void getParserReturnsTrueIfValidationChecksPass() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);
        GroupType parquetSchema = Types.requiredGroup()
                .required(BOOLEAN)
                .named("column-with-boolean-true-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ParquetBooleanParser parquetBooleanParser = new ParquetBooleanParser(simpleGroupValidation);

        boolean canHandle = parquetBooleanParser.canHandle(simpleGroup, "column-with-boolean-true-value");

        assertTrue(canHandle);
    }

    @Test
    public void deserializeShouldParseParquetBooleanToJavaBooleanType() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(BOOLEAN).named("column-with-boolean-true-value")
                .required(BOOLEAN).named("column-with-boolean-false-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("column-with-boolean-true-value", true);
        simpleGroup.add("column-with-boolean-false-value", false);

        ParquetBooleanParser booleanParser = new ParquetBooleanParser(simpleGroupValidation);
        Object actualValueForBooleanTrueColumn = booleanParser.deserialize(simpleGroup, "column-with-boolean-true-value");
        Object actualValueForBooleanFalseColumn = booleanParser.deserialize(simpleGroup, "column-with-boolean-false-value");

        assertEquals(true, actualValueForBooleanTrueColumn);
        assertEquals(false, actualValueForBooleanFalseColumn);
    }

    @Test
    public void deserializeShouldReturnDefaultFalseWhenDeserializedValueIsNull() {
        /* The field is added to the schema but not assigned a value */
        GroupType parquetSchema = Types.requiredGroup()
                .required(BOOLEAN).named("column-with-boolean-unset-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        ParquetBooleanParser booleanParser = new ParquetBooleanParser(simpleGroupValidation);
        Object actualValue = booleanParser.deserialize(simpleGroup, "column-with-boolean-unset-value");

        assertEquals(false, actualValue);
    }
}