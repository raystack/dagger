package org.raystack.dagger.common.serde.typehandler.primitive;

import com.google.common.primitives.Floats;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.raystack.dagger.common.serde.parquet.SimpleGroupValidation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.List;

/**
 * The type Float primitive type handler.
 */
public class FloatHandler implements PrimitiveHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Float primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public FloatHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.FLOAT;
    }

    @Override
    public Object parseObject(Object field) {
        return Float.parseFloat(getValueOrDefault(field, "0"));
    }

    @Override
    public Object parseSimpleGroup(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();

        /* this if branch checks that the field name exists in the simple group schema and is initialized */
        if (SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            return simpleGroup.getFloat(fieldName, 0);
        } else {
            /* return default value */
            return 0.0F;
        }
    }

    @Override
    public Object parseRepeatedObjectField(Object field) {

        float[] inputValues = new float[0];
        if (field != null) {
            inputValues = Floats.toArray((List<Float>) field);
        }
        return inputValues;
    }

    @Override
    public Object parseRepeatedSimpleGroupField(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();
        if (simpleGroup != null && SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            int repetitionCount = simpleGroup.getFieldRepetitionCount(fieldName);
            float[] floatArray = new float[repetitionCount];
            for (int i = 0; i < repetitionCount; i++) {
                floatArray[i] = simpleGroup.getFloat(fieldName, i);
            }
            return floatArray;
        }
        return new float[0];
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.FLOAT;
    }

    @Override
    public TypeInformation getArrayType() {
        return Types.PRIMITIVE_ARRAY(Types.FLOAT);
    }
}
