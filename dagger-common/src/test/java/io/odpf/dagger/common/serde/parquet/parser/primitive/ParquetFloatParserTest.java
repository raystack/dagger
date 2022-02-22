package io.odpf.dagger.common.serde.parquet.parser.primitive;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.junit.Assert.*;

public class ParquetFloatParserTest {

    @Test
    public void deserializeShouldParseParquetFloatToJavaFloatType() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(FLOAT).named("column-with-max-float-value")
                .required(FLOAT).named("column-with-min-float-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;
        simpleGroup.add("column-with-max-float-value", Float.MAX_VALUE);
        simpleGroup.add("column-with-min-float-value", Float.MIN_VALUE);

        ParquetFloatParser floatParser = new ParquetFloatParser();
        Object actualValueForMaxFloatColumn = floatParser.deserialize(simpleGroup, "column-with-max-float-value");
        Object actualValueForMinFloatColumn = floatParser.deserialize(simpleGroup, "column-with-min-float-value");

        assertEquals(Float.MAX_VALUE, actualValueForMaxFloatColumn);
        assertEquals(Float.MIN_VALUE, actualValueForMinFloatColumn);
    }

    @Test(expected = ClassCastException.class)
    public void deserializeShouldThrowExceptionWhenTypeExceeded() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(FLOAT).named("column-with-min-double-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;
        simpleGroup.add("column-with-min-double-value", Double.MIN_VALUE);


        ParquetFloatParser floatParser = new ParquetFloatParser();
        floatParser.deserialize(simpleGroup, "column-with-min-double-value");
    }

    @Test
    public void deserializeShouldReturnDefaultValueWhenSimpleGroupArgumentIsNull() {
        ParquetFloatParser parquetFloatParser = new ParquetFloatParser();
        Object actualValue = parquetFloatParser.deserialize(null, "some-random-field");

        assertEquals(0.0f, actualValue);
    }
}