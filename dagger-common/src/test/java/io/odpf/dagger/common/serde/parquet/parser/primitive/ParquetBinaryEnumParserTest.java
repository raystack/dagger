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

import java.time.DayOfWeek;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class ParquetBinaryEnumParserTest {
    @Mock
    SimpleGroupValidation simpleGroupValidation;

    @Before
    public void setup() {
        initMocks(this);
    }


    @Test
    public void canHandleReturnsFalseIfValidationChecksFail() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(false);
        ParquetBinaryEnumParser parquetEnumParser = new ParquetBinaryEnumParser(simpleGroupValidation);

        boolean canHandle = parquetEnumParser.canHandle(null, "column-with-enum-value");

        assertFalse(canHandle);
    }

    @Test
    public void getParserReturnsTrueIfValidationChecksPass() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);
        GroupType parquetSchema = Types.requiredGroup()
                .required(BINARY)
                .as(LogicalTypeAnnotation.enumType())
                .named("column-with-binary-enum-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        ParquetBinaryEnumParser parquetEnumParser = new ParquetBinaryEnumParser(simpleGroupValidation);

        boolean canHandle = parquetEnumParser.canHandle(simpleGroup, "column-with-binary-enum-type");

        assertTrue(canHandle);
    }


    @Test
    public void deserializeShouldParseParquetBinaryTypeWithLogicalTypeAnnotationAsEnumToString() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(BINARY)
                .as(LogicalTypeAnnotation.enumType())
                .named("column-with-binary-enum-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        String testEnumValue = DayOfWeek.FRIDAY.toString();

        simpleGroup.add("column-with-binary-enum-type", testEnumValue);
        ParquetBinaryEnumParser binaryEnumParser = new ParquetBinaryEnumParser(simpleGroupValidation);

        Object actualValue = binaryEnumParser.deserialize(simpleGroup, "column-with-binary-enum-type");

        assertEquals(DayOfWeek.FRIDAY.toString(), actualValue);
    }

    @Test
    public void deserializeShouldReturnDefaultValueWhenDeserializedValueIsNull() {
        /* The field is added to the schema but not assigned a value */
        GroupType parquetSchema = Types.requiredGroup()
                .required(BINARY)
                .as(LogicalTypeAnnotation.enumType())
                .named("some-random-field")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        ParquetBinaryEnumParser parquetBinaryEnumParser = new ParquetBinaryEnumParser(simpleGroupValidation);

        Object actualValue = parquetBinaryEnumParser.deserialize(simpleGroup, "some-random-field");

        assertEquals("null", actualValue);
    }
}