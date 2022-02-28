package io.odpf.dagger.common.serde.parquet.parser.validation;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.junit.Assert.*;

public class SimpleGroupValidationTest {

    @Test
    public void checkSimpleGroupNotNullReturnsTrueIfSimpleGroupArgumentIsNotNullElseFalse() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(BOOLEAN)
                .named("some-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        SimpleGroupValidation simpleGroupValidation = new SimpleGroupValidation();

        boolean actualResult1 = simpleGroupValidation.checkSimpleGroupNotNull(simpleGroup);
        boolean actualResult2 = simpleGroupValidation.checkSimpleGroupNotNull(null);

        assertTrue(actualResult1);
        assertFalse(actualResult2);
    }

    @Test
    public void checkFieldIsPrimitiveReturnsTrueIfFieldIsOfPrimitiveTypeElseFalse() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT32).named("primitive-type-column")
                .optionalGroup()
                .required(BOOLEAN).named("boolean-nested-column")
                .named("complex-type-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        SimpleGroupValidation simpleGroupValidation = new SimpleGroupValidation();

        boolean actualResultForPrimitive = simpleGroupValidation.checkFieldIsPrimitive(simpleGroup, "primitive-type-column");
        boolean actualResultForComplex = simpleGroupValidation.checkFieldIsPrimitive(simpleGroup, "complex-type-column");

        assertTrue(actualResultForPrimitive);
        assertFalse(actualResultForComplex);
    }

    @Test
    public void checkFieldExistsInSimpleGroupReturnsTrueIfSimpleGroupContainsTheFieldInItsSchemaElseFalse() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT32).named("primitive-type-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        SimpleGroupValidation simpleGroupValidation = new SimpleGroupValidation();

        boolean actualResultForExistingField = simpleGroupValidation.checkFieldExistsInSimpleGroup(simpleGroup, "primitive-type-column");
        boolean actualResultForNonExistingField = simpleGroupValidation.checkFieldExistsInSimpleGroup(simpleGroup, "this-column-does-not-exist");

        assertTrue(actualResultForExistingField);
        assertFalse(actualResultForNonExistingField);
    }

    @Test
    public void checkPrimitiveTypeIsSupportedReturnsTrueIfPrimitiveTypeNameIsSameAsProvidedArgumentElseFalse() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64).named("column-with-int64-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        SimpleGroupValidation simpleGroupValidation = new SimpleGroupValidation();

        boolean actualResultForSupportedField = simpleGroupValidation.checkParserCanParsePrimitive(simpleGroup, "column-with-int64-type", INT64);
        boolean actualResultForNonSupportedField = simpleGroupValidation.checkParserCanParsePrimitive(simpleGroup, "column-with-int64-type", BOOLEAN);

        assertTrue(actualResultForSupportedField);
        assertFalse(actualResultForNonSupportedField);
    }

    @Test
    public void checkLogicalTypeIsSupportedReturnsTrueIfLogicalTypeOfFieldNameIsSameAsProvidedArgumentElseFalse() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("column-with-timestamp-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        SimpleGroupValidation simpleGroupValidation = new SimpleGroupValidation();

        boolean actualResultForSupportedField = simpleGroupValidation.checkLogicalTypeIsSupported(
                simpleGroup, "column-with-timestamp-type",
                LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS));
        boolean actualResultForNonSupportedField = simpleGroupValidation.checkLogicalTypeIsSupported(
                simpleGroup, "column-with-timestamp-type",
                LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS));

        assertTrue(actualResultForSupportedField);
        assertFalse(actualResultForNonSupportedField);
    }

    @Test
    public void checkLogicalTypeIsSupportedReturnsTrueIfBothLogicalTypeOfFieldNameAndSupportedLogicalTypeIsNull() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64).named("column-with-int64-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        SimpleGroupValidation simpleGroupValidation = new SimpleGroupValidation();

        boolean actualResult = simpleGroupValidation.checkLogicalTypeIsSupported(simpleGroup, "column-with-int64-type", null);

        assertTrue(actualResult);
    }

    @Test
    public void applyValidationsReturnsTrueOnlyIfAllValidatorsReturnTrue() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64).named("column-with-int64-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        SimpleGroupValidation simpleGroupValidation = new SimpleGroupValidation();

        boolean actualResult = simpleGroupValidation.applyValidations(simpleGroup,
                "column-with-int64-type",
                INT64, null);

        assertTrue(actualResult);
    }

    @Test
    public void applyValidationsReturnsFalseIfCheckSimpleGroupNotNullValidationFails() {
        SimpleGroupValidation simpleGroupValidation = new SimpleGroupValidation();

        boolean actualResult = simpleGroupValidation.applyValidations(null, "some-column", INT64, null);

        assertFalse(actualResult);
    }

    @Test
    public void applyValidationsReturnsFalseIfCheckFieldExistsInSimpleGroupValidationFails() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64).named("column-with-int64-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        SimpleGroupValidation simpleGroupValidation = new SimpleGroupValidation();

        boolean actualResult = simpleGroupValidation.applyValidations(simpleGroup, "some-column", INT64, null);

        assertFalse(actualResult);
    }

    @Test
    public void applyValidationsReturnsFalseIfCheckFieldIsPrimitiveValidationFails() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT32).named("primitive-type-column")
                .optionalGroup()
                .required(BOOLEAN).named("boolean-nested-column")
                .named("complex-type-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        SimpleGroupValidation simpleGroupValidation = new SimpleGroupValidation();

        boolean actualResult = simpleGroupValidation.applyValidations(simpleGroup, "complex-type-column", INT64, null);

        assertFalse(actualResult);
    }

    @Test
    public void applyValidationsReturnsFalseIfCheckParserCanParsePrimitiveValidationFails() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64).named("column-with-int64-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        SimpleGroupValidation simpleGroupValidation = new SimpleGroupValidation();

        boolean actualResult = simpleGroupValidation.applyValidations(simpleGroup, "column-with-int64-type", BOOLEAN, null);

        assertFalse(actualResult);
    }

    @Test
    public void applyValidationsReturnsFalseIfCheckLogicalTypeIsSupportedValidationFails() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(BINARY).as(LogicalTypeAnnotation.enumType())
                .named("column-with-binary-enum-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        SimpleGroupValidation simpleGroupValidation = new SimpleGroupValidation();

        boolean actualResult = simpleGroupValidation.applyValidations(simpleGroup,
                "column-with-int64-type", BOOLEAN,
                LogicalTypeAnnotation.stringType());

        assertFalse(actualResult);
    }
}