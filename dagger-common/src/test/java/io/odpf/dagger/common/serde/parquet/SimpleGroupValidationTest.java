package io.odpf.dagger.common.serde.parquet;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.apache.parquet.schema.Types.optionalGroup;
import static org.apache.parquet.schema.Types.optionalMap;
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
    public void checkIsLegacySimpleGroupMapShouldOnlyReturnTrueWhenMapFieldConformsToTheLegacySchema() {
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

    @Test
    public void checkIsStandardSimpleGroupMapShouldReturnFalseWhenMapFieldIsNotOfTypeGroupType() {
        MessageType parquetSchema = buildMessage()
                .required(INT32).named("sample_map_field")
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkIsStandardSimpleGroupMap(simpleGroup, "sample_map_field"));
    }

    @Test
    public void checkIsStandardSimpleGroupMapShouldReturnFalseWhenMapFieldIsOfRepeatedType() {
        GroupType mapType = repeatedGroup()
                .required(INT32).named("key_value")
                .named("sample_map_field");

        MessageType parquetSchema = buildMessage()
                .addField(mapType)
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkIsStandardSimpleGroupMap(simpleGroup, "sample_map_field"));
    }

    @Test
    public void checkIsStandardSimpleGroupMapShouldReturnFalseWhenMapFieldDoesNotContainCorrectLogicalTypeAnnotation() {
        GroupType mapType = requiredGroup().as(LogicalTypeAnnotation.enumType())
                .required(INT32).named("key_value")
                .named("sample_map_field");

        MessageType parquetSchema = buildMessage()
                .addField(mapType)
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkIsStandardSimpleGroupMap(simpleGroup, "sample_map_field"));
    }

    @Test
    public void checkIsStandardSimpleGroupMapShouldReturnFalseWhenMapFieldDoesNotContainCorrectNumberOfNestedFields() {
        GroupType mapType = requiredGroup().as(LogicalTypeAnnotation.mapType())
                .required(INT32).named("key_value")
                .required(INT32).named("extra_field")
                .named("sample_map_field");

        MessageType parquetSchema = buildMessage()
                .addField(mapType)
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkIsStandardSimpleGroupMap(simpleGroup, "sample_map_field"));
    }

    @Test
    public void checkIsStandardSimpleGroupMapShouldReturnFalseWhenMapFieldDoesNotContainNestedKeyValueField() {
        GroupType mapType = requiredGroup().as(LogicalTypeAnnotation.mapType())
                .required(INT32).named("extra_field")
                .named("sample_map_field");

        MessageType parquetSchema = buildMessage()
                .addField(mapType)
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkIsStandardSimpleGroupMap(simpleGroup, "sample_map_field"));
    }

    @Test
    public void checkIsStandardSimpleGroupMapShouldReturnFalseWhenNestedKeyValueFieldIsNotAGroupType() {
        GroupType mapType = requiredGroup().as(LogicalTypeAnnotation.mapType())
                .required(INT32).named("key_value")
                .named("sample_map_field");

        MessageType parquetSchema = buildMessage()
                .addField(mapType)
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkIsStandardSimpleGroupMap(simpleGroup, "sample_map_field"));
    }

    @Test
    public void checkIsStandardSimpleGroupMapShouldReturnFalseWhenNestedKeyValueFieldIsNotRepeated() {
        GroupType keyValueType = optionalGroup()
                .required(INT32).named("some_key")
                .required(INT32).named("some_value")
                .named("key_value");

        GroupType mapType = requiredGroup().as(LogicalTypeAnnotation.mapType())
                .addField(keyValueType)
                .named("sample_map_field");

        MessageType parquetSchema = buildMessage()
                .addField(mapType)
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkIsStandardSimpleGroupMap(simpleGroup, "sample_map_field"));
    }

    @Test
    public void checkIsStandardSimpleGroupMapShouldReturnFalseWhenNestedKeyValueGroupDoesNotContainKeyField() {
        GroupType keyValueType = repeatedGroup()
                .required(INT32).named("some_key")
                .required(INT32).named("some_value")
                .named("key_value");

        GroupType mapType = requiredGroup().as(LogicalTypeAnnotation.mapType())
                .addField(keyValueType)
                .named("sample_map_field");

        MessageType parquetSchema = buildMessage()
                .addField(mapType)
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkIsStandardSimpleGroupMap(simpleGroup, "sample_map_field"));
    }

    @Test
    public void checkIsStandardSimpleGroupMapShouldReturnFalseWhenNestedKeyValueGroupDoesNotHaveKeyAsRequired() {
        GroupType keyValueType = repeatedGroup()
                .optional(INT32).named("key")
                .required(INT32).named("some_value")
                .named("key_value");

        GroupType mapType = requiredGroup().as(LogicalTypeAnnotation.mapType())
                .addField(keyValueType)
                .named("sample_map_field");

        MessageType parquetSchema = buildMessage()
                .addField(mapType)
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertFalse(SimpleGroupValidation.checkIsStandardSimpleGroupMap(simpleGroup, "sample_map_field"));
    }

    @Test
    public void checkIsStandardSimpleGroupMapShouldOnlyReturnTrueWhenMapFieldConformsToTheStandardSpec() {
        GroupType mapType = optionalMap()
                .key(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                .optionalValue(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                .named("sample_map_field");

        MessageType parquetSchema = buildMessage()
                .addField(mapType)
                .named("TestMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertTrue(SimpleGroupValidation.checkIsStandardSimpleGroupMap(simpleGroup, "sample_map_field"));
    }
}
