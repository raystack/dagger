package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.exceptions.serde.DaggerDeserializationException;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.time.DayOfWeek;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.assertEquals;

public class ParquetBinaryStringParserTest {
    @Test
    public void deserializeShouldParseParquetBinaryTypeWithLogicalTypeAnnotationAsStringToJavaString() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("column-with-binary-string-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;
        simpleGroup.add("column-with-binary-string-type", "some-random-string");
        ParquetBinaryStringParser binaryParser = new ParquetBinaryStringParser();

        Object actualValue = binaryParser.deserialize(simpleGroup, "column-with-binary-string-type");

        assertEquals("some-random-string", actualValue);
    }

    @Test(expected = DaggerDeserializationException.class)
    public void deserializeShouldThrowExceptionWhenLogicalTypeAnnotationIsMissing() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(BINARY)
                .named("column-with-binary-string-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("column-with-binary-string-type", "some string value");

        ParquetBinaryStringParser binaryParser = new ParquetBinaryStringParser();
        binaryParser.deserialize(simpleGroup, "column-with-binary-string-type");
    }

    @Test(expected = DaggerDeserializationException.class)
    public void deserializeShouldThrowExceptionWhenLogicalTypeAnnotationIsNotString() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(BINARY)
                .as(LogicalTypeAnnotation.enumType())
                .named("column-with-binary-enum-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        String testEnumValue = DayOfWeek.MONDAY.toString();
        simpleGroup.add("column-with-binary-enum-type", testEnumValue);

        ParquetBinaryStringParser binaryParser = new ParquetBinaryStringParser();
        binaryParser.deserialize(simpleGroup, "column-with-binary-enum-type");
    }

    @Test
    public void deserializeShouldReturnDefaultValueWhenSimpleGroupArgumentIsNull() {
        ParquetBinaryStringParser parquetBinaryStringParser = new ParquetBinaryStringParser();
        Object actualValue = parquetBinaryStringParser.deserialize(null, "some-random-field");

        assertEquals("", actualValue);
    }
}