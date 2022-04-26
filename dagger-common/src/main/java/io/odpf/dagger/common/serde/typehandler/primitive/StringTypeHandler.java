package io.odpf.dagger.common.serde.typehandler.primitive;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import io.odpf.dagger.common.serde.parquet.SimpleGroupValidation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * The type String primitive type handler.
 */
public class StringTypeHandler implements PrimitiveHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new String primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public StringTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
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
    public TypeInformation getTypeInformation() {
        return Types.STRING;
    }

    @Override
    public TypeInformation getArrayType() {
        return ObjectArrayTypeInfo.getInfoFor(Types.STRING);
    }
}
