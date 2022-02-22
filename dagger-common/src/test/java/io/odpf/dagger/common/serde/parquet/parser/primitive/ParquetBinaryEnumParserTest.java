package io.odpf.dagger.common.serde.parquet.parser.primitive;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.time.DayOfWeek;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.assertEquals;


public class ParquetBinaryEnumParserTest {

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
        ParquetBinaryEnumParser binaryEnumParser = new ParquetBinaryEnumParser();

        Object actualValue = binaryEnumParser.deserialize(simpleGroup, "column-with-binary-enum-type");

        assertEquals(DayOfWeek.FRIDAY.toString(), actualValue);
    }

    @Test(expected = ClassCastException.class)
    public void deserializeShouldThrowExceptionWhenLogicalTypeAnnotationIsNotEnum() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("column-with-binary-string-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("column-with-binary-string-type", "some random string");

        ParquetBinaryEnumParser binaryEnumParser = new ParquetBinaryEnumParser();
        binaryEnumParser.deserialize(simpleGroup, "column-with-binary-string-type");
    }

    @Test
    public void deserializeShouldReturnDefaultValueWhenSimpleGroupArgumentIsNull() {
        ParquetBinaryEnumParser parquetBinaryEnumParser = new ParquetBinaryEnumParser();
        Object actualValue = parquetBinaryEnumParser.deserialize(null, "some-random-field");

        assertEquals("null", actualValue);
    }
}