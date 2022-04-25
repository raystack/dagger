package io.odpf.dagger.common.serde.parquet;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleGroupValidationTest {

    @Test
    public void checkFieldExistsAndIsInitializedShouldReturnFalseWhenFieldIsNotInitializedInSimpleGroup() {
        GroupType parquetSchema = buildMessage()
                .required(INT32).named("primitive-type-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, "primitive-type-column"));
    }

    @Test
    public void checkFieldExistsAndIsInitializedShouldReturnFalseWhenFieldIsNotPresentInSimpleGroup() {
        GroupType parquetSchema = buildMessage()
                .required(INT32).named("primitive-type-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("primitive-type-column", 34);

        assertFalse(SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, "unknown-column"));
    }

    @Test
    public void checkFieldExistsAndIsInitializedShouldReturnTrueWhenFieldIsBothPresentAndInitializedInSimpleGroup() {
        GroupType parquetSchema = buildMessage()
                .required(INT32).named("primitive-type-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("primitive-type-column", 34);

        assertTrue(SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, "primitive-type-column"));
    }
}
