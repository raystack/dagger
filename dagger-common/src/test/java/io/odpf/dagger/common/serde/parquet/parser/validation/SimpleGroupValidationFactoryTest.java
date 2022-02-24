package io.odpf.dagger.common.serde.parquet.parser.validation;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.junit.Assert.*;

public class SimpleGroupValidationFactoryTest {

    @Test
    public void checkSimpleGroupNotNullReturnsTrueIfSimpleGroupArgumentIsNotNullElseFalse() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(BOOLEAN)
                .named("some-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        boolean actualResult1 = SimpleGroupValidationFactory.checkSimpleGroupNotNull(simpleGroup);
        boolean actualResult2 = SimpleGroupValidationFactory.checkSimpleGroupNotNull(null);

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

        boolean actualResultForPrimitive = SimpleGroupValidationFactory.checkFieldIsPrimitive(simpleGroup, "primitive-type-column");
        boolean actualResultForComplex = SimpleGroupValidationFactory.checkFieldIsPrimitive(simpleGroup, "complex-type-column");

        assertTrue(actualResultForPrimitive);
        assertFalse(actualResultForComplex);
    }

    @Test
    public void checkFieldExistsInSimpleGroupReturnsTrueIfSimpleGroupContainsTheFieldInItsSchemaElseFalse() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT32).named("primitive-type-column")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        boolean actualResultForExistingField = SimpleGroupValidationFactory.checkFieldExistsInSimpleGroup(simpleGroup, "primitive-type-column");
        boolean actualResultForNonExistingField = SimpleGroupValidationFactory.checkFieldExistsInSimpleGroup(simpleGroup, "this-column-does-not-exist");

        assertTrue(actualResultForExistingField);
        assertFalse(actualResultForNonExistingField);
    }

    @Test
    public void checkLogicalTypeIsSupportedReturnsTrueIfLogicalTypeOfFieldNameIsSameAsProvidedArgumentElseFalse() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(BINARY).as(LogicalTypeAnnotation.enumType()).named("column-with-binary-enum-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        boolean actualResultForSupportedField = SimpleGroupValidationFactory.checkLogicalTypeIsSupported(simpleGroup, "column-with-binary-enum-type", LogicalTypeAnnotation.enumType());
        boolean actualResultForNonSupportedField = SimpleGroupValidationFactory.checkLogicalTypeIsSupported(simpleGroup, "column-with-binary-enum-type", LogicalTypeAnnotation.stringType());

        assertTrue(actualResultForSupportedField);
        assertFalse(actualResultForNonSupportedField);
    }

    @Test
    public void checkPrimitiveTypeIsSupported() {
        GroupType parquetSchema = Types.requiredGroup()
                .required(INT64).named("column-with-int64-type")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        boolean actualResultForSupportedField = SimpleGroupValidationFactory.checkPrimitiveTypeIsSupported(simpleGroup, "column-with-int64-type", INT64);
        boolean actualResultForNonSupportedField = SimpleGroupValidationFactory.checkPrimitiveTypeIsSupported(simpleGroup, "column-with-int64-type", BOOLEAN);

        assertTrue(actualResultForSupportedField);
        assertFalse(actualResultForNonSupportedField);
    }
}