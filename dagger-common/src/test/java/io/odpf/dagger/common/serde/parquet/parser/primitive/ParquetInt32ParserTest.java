package io.odpf.dagger.common.serde.parquet.parser.primitive;

import io.odpf.dagger.common.serde.parquet.SimpleGroupValidation;
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

public class ParquetInt32ParserTest {

    @Mock
    SimpleGroupValidation simpleGroupValidation;

    @Before
    public void setup() {
        initMocks(this);
    }


    @Test
    public void canHandleReturnsFalseIfValidationChecksFail() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(false);
        ParquetInt32Parser parquetInt32Parser = new ParquetInt32Parser(simpleGroupValidation);

        boolean canHandle = parquetInt32Parser.canHandle(null, "column-with-int32-value");

        assertFalse(canHandle);
    }

    @Test
    public void getParserReturnsTrueIfValidationChecksPass() {
        when(simpleGroupValidation.applyValidations(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT32)
                .named("column-with-int32-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        ParquetInt32Parser parquetInt32Parser = new ParquetInt32Parser(simpleGroupValidation);

        boolean canHandle = parquetInt32Parser.canHandle(simpleGroup, "column-with-int32-value");

        assertTrue(canHandle);
    }

    @Test
    public void deserializeShouldParseParquetInt32ToJavaIntType() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT32).named("column-with-max-int-value")
                .required(INT32).named("column-with-min-int-value")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema) ;
        simpleGroup.add("column-with-max-int-value", Integer.MAX_VALUE);
        simpleGroup.add("column-with-min-int-value", Integer.MIN_VALUE);

        ParquetInt32Parser int32Parser = new ParquetInt32Parser(simpleGroupValidation);
        Object actualValueForMaxIntColumn = int32Parser.deserialize(simpleGroup, "column-with-max-int-value");
        Object actualValueForMinIntColumn = int32Parser.deserialize(simpleGroup, "column-with-min-int-value");

        assertEquals(Integer.MAX_VALUE, actualValueForMaxIntColumn);
        assertEquals(Integer.MIN_VALUE, actualValueForMinIntColumn);
    }

    @Test
    public void deserializeShouldReturnDefaultValueWhenDeserializedValueIsNull() {
        /* The field is added to the schema but not assigned a value */
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT32)
                .named("some-random-field")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        ParquetInt32Parser parquetInt32Parser = new ParquetInt32Parser(simpleGroupValidation);

        Object actualValue = parquetInt32Parser.deserialize(simpleGroup, "some-random-field");

        assertEquals(0, actualValue);
    }
}