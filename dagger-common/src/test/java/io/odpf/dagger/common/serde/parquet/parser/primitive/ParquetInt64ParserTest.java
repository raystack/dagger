package io.odpf.dagger.common.serde.parquet.parser.primitive;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.*;

public class ParquetInt64ParserTest {
    @Test
    public void deserializeShouldParseParquetInt64ToJavaLongType() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64).named("column-with-max-long-value")
                .required(INT64).named("column-with-min-long-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("column-with-max-long-value", Long.MAX_VALUE);
        simpleGroup.add("column-with-min-long-value", Long.MIN_VALUE);

        ParquetInt64Parser int64Parser = new ParquetInt64Parser();
        Object actualValueForMaxLongColumn = int64Parser.deserialize(simpleGroup, "column-with-max-long-value");
        Object actualValueForMinLongColumn = int64Parser.deserialize(simpleGroup, "column-with-min-long-value");

        assertEquals(Long.MAX_VALUE, actualValueForMaxLongColumn);
        assertEquals(Long.MIN_VALUE, actualValueForMinLongColumn);
    }

    @Test
    public void deserializeShouldReturnDefaultValueZeroWhenSimpleGroupArgumentIsNull() {
        ParquetInt64Parser parquetInt64Parser = new ParquetInt64Parser();
        Object actualValue = parquetInt64Parser.deserialize(null, "some-random-field");

        assertEquals(0L, actualValue);
    }
}