package org.raystack.dagger.common.serde.typehandler.primitive;

import com.google.common.primitives.Ints;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.raystack.dagger.common.serde.parquet.SimpleGroupValidation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.List;

/**
 * The type Integer primitive type handler.
 */
public class IntegerHandler implements PrimitiveHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Integer primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public IntegerHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.INT;
    }

    @Override
    public Object parseObject(Object field) {
        return Integer.parseInt(getValueOrDefault(field, "0"));
    }

    @Override
    public Object parseSimpleGroup(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();

        /* this if branch checks that the field name exists in the simple group schema and is initialized */
        if (SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            return simpleGroup.getInteger(fieldName, 0);
        } else {
            /* return default value */
            return 0;
        }
    }

    @Override
    public Object parseRepeatedObjectField(Object field) {
        int[] inputValues = new int[0];
        if (field != null) {
            inputValues = Ints.toArray((List<Integer>) field);
        }
        return inputValues;
    }

    @Override
    public Object parseRepeatedSimpleGroupField(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();
        if (simpleGroup != null && SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            int repetitionCount = simpleGroup.getFieldRepetitionCount(fieldName);
            int[] intArray = new int[repetitionCount];
            for (int i = 0; i < repetitionCount; i++) {
                intArray[i] = simpleGroup.getInteger(fieldName, i);
            }
            return intArray;
        }
        return new int[0];
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.INT;
    }

    @Override
    public TypeInformation getArrayType() {
        return Types.PRIMITIVE_ARRAY(Types.INT);
    }
}
