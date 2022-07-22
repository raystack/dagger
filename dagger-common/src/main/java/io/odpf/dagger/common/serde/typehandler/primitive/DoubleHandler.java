package io.odpf.dagger.common.serde.typehandler.primitive;

import com.google.common.primitives.Doubles;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import io.odpf.dagger.common.serde.parquet.SimpleGroupValidation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.List;

/**
 * The type Double primitive type handler.
 */
public class DoubleHandler implements PrimitiveHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Double primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public DoubleHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.DOUBLE;
    }

    @Override
    public Object parseObject(Object field) {
        return Double.parseDouble(getValueOrDefault(field, "0"));
    }

    @Override
    public Object parseSimpleGroup(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();

        /* this if branch checks that the field name exists in the simple group schema and is initialized */
        if (SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            return simpleGroup.getDouble(fieldName, 0);
        } else {
            /* return default value */
            return 0.0D;
        }
    }

    @Override
    public Object parseRepeatedObjectField(Object field) {
        double[] inputValues = new double[0];
        if (field != null) {
            inputValues = Doubles.toArray((List<Double>) field);
        }
        return inputValues;
    }

    @Override
    public Object parseRepeatedSimpleGroupField(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();
        if (simpleGroup != null && SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            int repetitionCount = simpleGroup.getFieldRepetitionCount(fieldName);
            double[] doubleArray = new double[repetitionCount];
            for (int i = 0; i < repetitionCount; i++) {
                doubleArray[i] = simpleGroup.getDouble(fieldName, i);
            }
            return doubleArray;
        }
        return new double[0];
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.DOUBLE;
    }

    @Override
    public TypeInformation getArrayType() {
        return Types.PRIMITIVE_ARRAY(Types.DOUBLE);
    }
}
