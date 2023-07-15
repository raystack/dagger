package org.raystack.dagger.common.serde.typehandler.primitive;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.raystack.dagger.common.serde.parquet.SimpleGroupValidation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * The type String primitive type handler.
 */
public class StringHandler implements PrimitiveHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new String primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public StringHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.STRING;
    }

    @Override
    public Object parseObject(Object field) {
        return getValueOrDefault(field, "");
    }

    @Override
    public Object parseSimpleGroup(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();

        /* this if branch checks that the field name exists in the simple group schema and is initialized */
        if (SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            return simpleGroup.getString(fieldName, 0);
        } else {
            /* return default value */
            return "";
        }
    }

    @Override
    public Object parseRepeatedObjectField(Object field) {
        List<String> inputValues = new ArrayList<>();
        if (field != null) {
            inputValues = (List<String>) field;
        }
        return inputValues.toArray(new String[]{});
    }

    @Override
    public Object parseRepeatedSimpleGroupField(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();
        if (simpleGroup != null && SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            int repetitionCount = simpleGroup.getFieldRepetitionCount(fieldName);
            String[] stringArray = new String[repetitionCount];
            for (int i = 0; i < repetitionCount; i++) {
                stringArray[i] = simpleGroup.getString(fieldName, i);
            }
            return stringArray;
        }
        return new String[0];
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.STRING;
    }

    @Override
    public TypeInformation getArrayType() {
        return ObjectArrayTypeInfo.getInfoFor(Types.STRING);
    }
}
