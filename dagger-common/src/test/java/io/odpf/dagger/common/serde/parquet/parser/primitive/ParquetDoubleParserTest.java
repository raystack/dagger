package io.odpf.dagger.common.serde.parquet.parser.primitive;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.junit.Assert.*;

public class ParquetDoubleParserTest {

    @Test
    public void deserializeShouldParseParquetDoubleToJavaDoubleType() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(DOUBLE).named("column-with-max-double-value")
                .required(DOUBLE).named("column-with-min-double-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;
        simpleGroup.add("column-with-max-double-value", Double.MAX_VALUE);
        simpleGroup.add("column-with-min-double-value", Double.MIN_VALUE);

        ParquetDoubleParser doubleParser = new ParquetDoubleParser();
        Object actualValueForMaxDoubleColumn = doubleParser.deserialize(simpleGroup, "column-with-max-double-value");
        Object actualValueForMinDoubleColumn = doubleParser.deserialize(simpleGroup, "column-with-min-double-value");

        assertEquals(Double.MAX_VALUE, actualValueForMaxDoubleColumn);
        assertEquals(Double.MIN_VALUE, actualValueForMinDoubleColumn);
    }

    @Test
    public void deserializeShouldReturnDefaultValueWhenSimpleGroupArgumentIsNull() {
        ParquetDoubleParser parquetDoubleParser = new ParquetDoubleParser();
        Object actualValue = parquetDoubleParser.deserialize(null, "some-random-field");

        assertEquals(0.0d, actualValue);
    }
}