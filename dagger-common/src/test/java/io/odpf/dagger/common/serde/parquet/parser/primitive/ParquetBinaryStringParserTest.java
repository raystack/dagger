package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.serde.parquet.SimpleGroupValidation;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;


import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ParquetBinaryStringParserTest {
    @Mock
    SimpleGroupValidation simpleGroupValidation;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void canHandleReturnsFalseIfValidationChecksFail() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(false);
        ParquetBinaryStringParser parquetBinaryStringParser = new ParquetBinaryStringParser(simpleGroupValidation);

        boolean canHandle = parquetBinaryStringParser.canHandle(null, "column-with-binary-string-value");

        assertFalse(canHandle);
    }

    @Test
    public void getParserReturnsTrueIfValidationChecksPass() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);
        GroupType parquetSchema = Types.requiredGroup()
                .required(BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("column-with-binary-string-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;

        ParquetBinaryStringParser parquetBinaryStringParser = new ParquetBinaryStringParser(simpleGroupValidation);

        boolean canHandle = parquetBinaryStringParser.canHandle(simpleGroup, "column-with-binary-string-type");

        assertTrue(canHandle);
    }

    @Test
    public void deserializeShouldParseParquetBinaryTypeWithLogicalTypeAnnotationAsStringToJavaString() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("column-with-binary-string-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;
        simpleGroup.add("column-with-binary-string-type", "some-random-string");
        ParquetBinaryStringParser binaryParser = new ParquetBinaryStringParser(simpleGroupValidation);

        Object actualValue = binaryParser.deserialize(simpleGroup, "column-with-binary-string-type");

        assertEquals("some-random-string", actualValue);
    }

    @Test
    public void deserializeShouldReturnDefaultValueWhenDeserializedValueIsNull() {
        /* The field is added to the schema but not assigned a value */
        GroupType parquetSchema = Types.requiredGroup()
                .required(BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("column-with-binary-string-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;
        ParquetBinaryStringParser parquetBinaryStringParser = new ParquetBinaryStringParser(simpleGroupValidation);

        Object actualValue = parquetBinaryStringParser.deserialize(simpleGroup, "column-with-binary-string-type");

        assertEquals("", actualValue);
    }
}