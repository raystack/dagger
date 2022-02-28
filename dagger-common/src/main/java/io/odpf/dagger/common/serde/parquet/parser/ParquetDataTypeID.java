package io.odpf.dagger.common.serde.parquet.parser;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Objects;

public class ParquetDataTypeID {
    private final PrimitiveType.PrimitiveTypeName primitiveTypeName;
    private final LogicalTypeAnnotation logicalType;

    ParquetDataTypeID(PrimitiveType.PrimitiveTypeName primitiveTypeName, LogicalTypeAnnotation logicalType) {
        this.primitiveTypeName = primitiveTypeName;
        this.logicalType = logicalType;
    }

    @Override
    public String toString() {
        String logicalTypeName = (logicalType == null)
                ? "" :
                (String.format("_%s", logicalType));
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(primitiveTypeName.toString());
        stringBuilder.append(logicalTypeName);
        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParquetDataTypeID that = (ParquetDataTypeID) o;
        return primitiveTypeName == that.primitiveTypeName && Objects.equals(logicalType, that.logicalType);
    }
}