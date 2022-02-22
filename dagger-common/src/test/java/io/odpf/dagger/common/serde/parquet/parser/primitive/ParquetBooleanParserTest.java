package io.odpf.dagger.common.serde.parquet.parser.primitive;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.junit.Assert.assertEquals;

public class ParquetBooleanParserTest {

    @Test
    public void deserializeShouldParseParquetBooleanToJavaBooleanType() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(BOOLEAN).named("column-with-boolean-true-value")
                .required(BOOLEAN).named("column-with-boolean-false-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("column-with-boolean-true-value", true);
        simpleGroup.add("column-with-boolean-false-value", false);

        ParquetBooleanParser booleanParser = new ParquetBooleanParser();
        Object actualValueForBooleanTrueColumn = booleanParser.deserialize(simpleGroup, "column-with-boolean-true-value");
        Object actualValueForBooleanFalseColumn = booleanParser.deserialize(simpleGroup, "column-with-boolean-false-value");

        assertEquals(true, actualValueForBooleanTrueColumn);
        assertEquals(false, actualValueForBooleanFalseColumn);
    }

    @Test
    public void deserializeShouldReturnDefaultFalseWhenSimpleGroupArgumentIsNull() {
        ParquetBooleanParser booleanParser = new ParquetBooleanParser();
        Object actualValue = booleanParser.deserialize(null, "some-random-field");

        assertEquals(false, actualValue);
    }
}