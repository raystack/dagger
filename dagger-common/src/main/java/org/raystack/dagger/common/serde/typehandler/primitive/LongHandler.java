package org.raystack.dagger.common.serde.typehandler.primitive;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.raystack.dagger.common.serde.parquet.SimpleGroupValidation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * The type Long primitive type handler.
 */
public class LongHandler implements PrimitiveHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Long primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public LongHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.LONG;
    }

    @Override
    public Object parseObject(Object field) {
        return Long.parseLong(getValueOrDefault(field, "0"));
    }

    @Override
    public Object parseSimpleGroup(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();

        /* this if branch checks that the field name exists in the simple group schema and is initialized */
        if (SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            return simpleGroup.getLong(fieldName, 0);
        } else {
            /* return default value */
            return 0L;
        }
    }

    @Override
    public Object parseRepeatedObjectField(Object field) {
        List<Long> inputValues = new ArrayList<>();
        if (field != null) {
            inputValues = (List<Long>) field;
        }
        return inputValues.toArray(new Long[]{});
    }

    @Override
    public Object parseRepeatedSimpleGroupField(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();
        ArrayList<Long> longArrayList = new ArrayList<>();
        if (simpleGroup != null && SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            int repetitionCount = simpleGroup.getFieldRepetitionCount(fieldName);
            for (int i = 0; i < repetitionCount; i++) {
                longArrayList.add(simpleGroup.getLong(fieldName, i));
            }
        }
        return longArrayList.toArray(new Long[]{});
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.LONG;
    }

    @Override
    public TypeInformation getArrayType() {
        return Types.OBJECT_ARRAY(Types.LONG);
    }
}
