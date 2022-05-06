package io.odpf.dagger.common.serde.parquet;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.apache.parquet.schema.Types.repeatedGroup;
import static org.apache.parquet.schema.Types.requiredGroup;
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

    @Test
    public void checkIsLegacySimpleGroupMapShouldReturnFalseWhenMapFieldIsNotOfTypeGroupType() {
        MessageType parquetSchema = buildMessage()
                .required(INT32).named("sample_map_field")
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkIsLegacySimpleGroupMap(simpleGroup, "sample_map_field"));
    }

    @Test
    public void checkIsLegacySimpleGroupMapShouldReturnFalseWhenMapFieldIsNotRepeated() {
        GroupType mapSchema = requiredGroup()
                .optional(INT32).named("key")
                .optional(FLOAT).named("value")
                .named("sample_map_field");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkIsLegacySimpleGroupMap(simpleGroup, "sample_map_field"));
    }

    @Test
    public void checkIsLegacySimpleGroupMapShouldReturnFalseWhenMapFieldSchemaDoesNotHaveCorrectNumberOfNestedFields() {
        GroupType mapSchema = repeatedGroup()
                .optional(INT32).named("key")
                .optional(FLOAT).named("value")
                .optional(DOUBLE).named("extra_field")
                .named("sample_map_field");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkIsLegacySimpleGroupMap(simpleGroup, "sample_map_field"));
    }

    @Test
    public void checkIsLegacySimpleGroupMapShouldReturnFalseWhenMapFieldSchemaDoesNotHaveKey() {
        GroupType mapSchema = repeatedGroup()
                .optional(FLOAT).named("value")
                .optional(DOUBLE).named("extra_field")
                .named("sample_map_field");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkIsLegacySimpleGroupMap(simpleGroup, "sample_map_field"));
    }

    @Test
    public void checkIsLegacySimpleGroupMapShouldReturnFalseWhenMapFieldSchemaDoesNotHaveValue() {
        GroupType mapSchema = repeatedGroup()
                .optional(FLOAT).named("key")
                .optional(DOUBLE).named("extra_field")
                .named("sample_map_field");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkIsLegacySimpleGroupMap(simpleGroup, "sample_map_field"));
    }

    @Test
    public void checkIsLegacySimpleGroupMapShouldReturnTrueWhenMapFieldConformsToTheLegacySchema() {
        GroupType mapSchema = repeatedGroup()
                .optional(INT32).named("key")
                .optional(FLOAT).named("value")
                .named("sample_map_field");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertTrue(SimpleGroupValidation.checkIsLegacySimpleGroupMap(simpleGroup, "sample_map_field"));
    }
}
