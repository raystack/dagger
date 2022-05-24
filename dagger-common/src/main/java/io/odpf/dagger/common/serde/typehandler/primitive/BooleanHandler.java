package io.odpf.dagger.common.serde.typehandler.primitive;

import com.google.common.primitives.Booleans;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.parquet.example.data.simple.SimpleGroup;
import io.odpf.dagger.common.serde.parquet.SimpleGroupValidation;

import java.util.List;

/**
 * The type Boolean primitive type handler.
 */
public class BooleanHandler implements PrimitiveHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Boolean primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public BooleanHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.BOOLEAN;
    }

    @Override
    public Object parseObject(Object field) {
        return Boolean.parseBoolean(getValueOrDefault(field, "false"));
    }

    @Override
    public Object parseSimpleGroup(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();

        /* this if branch checks that the field name exists in the simple group schema and is initialized */
        if (SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            return simpleGroup.getBoolean(fieldName, 0);
        } else {
            /* return default value */
            return false;
        }
    }

    @Override
    public Object parseRepeatedObjectField(Object field) {
        boolean[] inputValues = new boolean[0];
        if (field != null) {
            inputValues = Booleans.toArray((List<Boolean>) field);
        }
        return inputValues;
    }

    @Override
    public Object parseRepeatedSimpleGroupField(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();
        if (simpleGroup != null && SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            int repetitionCount = simpleGroup.getFieldRepetitionCount(fieldName);
            boolean[] booleanArray = new boolean[repetitionCount];
            for (int i = 0; i < repetitionCount; i++) {
                booleanArray[i] = simpleGroup.getBoolean(fieldName, i);
            }
            return booleanArray;
        }
        return new boolean[0];
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.BOOLEAN;
    }

    @Override
    public TypeInformation getArrayType() {
        return Types.PRIMITIVE_ARRAY(Types.BOOLEAN);
    }
}
