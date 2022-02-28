package io.odpf.dagger.common.serde.parquet.parser;


import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.junit.Assert.*;

public class ParquetDataTypeIDTest {

    @Test
    public void equalsShouldReturnTrueForObjectsWithSamePrimitiveTypeAndLogicalType() {
        ParquetDataTypeID parquetDataTypeID1 = new ParquetDataTypeID(BOOLEAN, null);
        ParquetDataTypeID parquetDataTypeID2 = new ParquetDataTypeID(BOOLEAN, null);

        boolean isEqual = parquetDataTypeID1.equals(parquetDataTypeID2);

        assertTrue(isEqual);
    }

    @Test
    public void equalsShouldReturnFalseForObjectsWithDifferentPrimitiveTypeButSameLogicalType() {
        ParquetDataTypeID parquetDataTypeID1 = new ParquetDataTypeID(BOOLEAN, null);
        ParquetDataTypeID parquetDataTypeID2 = new ParquetDataTypeID(INT32, null);

        boolean isEqual = parquetDataTypeID1.equals(parquetDataTypeID2);

        assertFalse(isEqual);
    }

    @Test
    public void equalsShouldReturnFalseForObjectsWithSamePrimitiveTypeButDifferentLogicalType() {
        ParquetDataTypeID parquetDataTypeID1 = new ParquetDataTypeID(INT64,
                LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS));
        ParquetDataTypeID parquetDataTypeID2 = new ParquetDataTypeID(INT64,
                LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS));

        boolean isEqual = parquetDataTypeID1.equals(parquetDataTypeID2);

        assertFalse(isEqual);
    }

    @Test
    public void equalsShouldReturnFalseForObjectsWithSameLogicalTypeButDifferentPrimitiveType() {
        ParquetDataTypeID parquetDataTypeID1 = new ParquetDataTypeID(INT64, LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS));
        ParquetDataTypeID parquetDataTypeID2 = new ParquetDataTypeID(INT32, LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS));

        boolean isEqual = parquetDataTypeID1.equals(parquetDataTypeID2);

        assertFalse(isEqual);
    }

    @Test
    public void equalsShouldReturnFalseForObjectsOfDifferentObjectType() {
        ParquetDataTypeID parquetDataTypeID1 = new ParquetDataTypeID(INT64, LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS));
        Long someLongObject = 23L;

        boolean isEqual = parquetDataTypeID1.equals(someLongObject);

        assertFalse(isEqual);
    }

    @Test
    public void equalsShouldReturnFalseWhenComparedObjectIsNull() {
        ParquetDataTypeID parquetDataTypeID1 = new ParquetDataTypeID(INT64, LogicalTypeAnnotation.enumType());

        boolean isEqual = parquetDataTypeID1.equals(null);

        assertFalse(isEqual);
    }

    @Test
    public void toStringPrintsPrimitiveTypeOnlyWhenLogicalTypeIsNull() {
        ParquetDataTypeID parquetDataTypeID = new ParquetDataTypeID(INT64, null);

        String actualString = parquetDataTypeID.toString();

        assertEquals("INT64", actualString);
    }

    @Test
    public void toStringPrintsBothPrimitiveTypeAndLogicalType() {
        ParquetDataTypeID parquetDataTypeID = new ParquetDataTypeID(BINARY, LogicalTypeAnnotation.enumType());

        String actualString = parquetDataTypeID.toString();

        assertEquals("BINARY_ENUM", actualString);
    }

    @Test
    public void toStringPrintsBothPrimitiveTypeAndLogicalTypeWithParameters() {
        ParquetDataTypeID parquetDataTypeID = new ParquetDataTypeID(INT64, LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS));

        String actualString = parquetDataTypeID.toString();

        assertEquals("INT64_TIMESTAMP(NANOS,true)", actualString);
    }
}