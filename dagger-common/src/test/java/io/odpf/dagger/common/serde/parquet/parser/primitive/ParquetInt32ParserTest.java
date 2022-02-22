package io.odpf.dagger.common.serde.parquet.parser.primitive;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.junit.Assert.assertEquals;

public class ParquetInt32ParserTest {

    @Test
    public void deserializeShouldParseParquetInt32ToJavaIntType() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT32).named("column-with-max-int-value")
                .required(INT32).named("column-with-min-int-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;
        simpleGroup.add("column-with-max-int-value", Integer.MAX_VALUE);
        simpleGroup.add("column-with-min-int-value", Integer.MIN_VALUE);

        ParquetInt32Parser int32Parser = new ParquetInt32Parser();
        Object actualValueForMaxIntColumn = int32Parser.deserialize(simpleGroup, "column-with-max-int-value");
        Object actualValueForMinIntColumn = int32Parser.deserialize(simpleGroup, "column-with-min-int-value");

        assertEquals(Integer.MAX_VALUE, actualValueForMaxIntColumn);
        assertEquals(Integer.MIN_VALUE, actualValueForMinIntColumn);
    }

    @Test(expected = ClassCastException.class)
    public void deserializeShouldThrowExceptionWhenTypeRangeExceeded() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64).named("column-with-max-long-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;
        simpleGroup.add("column-with-max-long-value", Long.MAX_VALUE);

        ParquetInt32Parser int32Parser = new ParquetInt32Parser();
        int32Parser.deserialize(simpleGroup, "column-with-max-long-value");
    }

    @Test
    public void deserializeShouldReturnDefaultValueZeroWhenSimpleGroupArgumentIsNull() {
        ParquetInt32Parser parquetInt32Parser = new ParquetInt32Parser();
        Object actualValue = parquetInt32Parser.deserialize(null, "some-random-field");

        assertEquals(0, actualValue);
    }
}