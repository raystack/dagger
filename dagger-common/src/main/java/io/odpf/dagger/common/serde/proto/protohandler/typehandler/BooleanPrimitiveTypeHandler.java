package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import com.google.common.primitives.Booleans;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.List;

/**
 * The type Boolean primitive type handler.
 */
public class BooleanPrimitiveTypeHandler implements PrimitiveTypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Boolean primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public BooleanPrimitiveTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.BOOLEAN;
    }

    @Override
    public Object getValue(Object field) {
        return Boolean.parseBoolean(getValueOrDefault(field, "false"));
    }

    @Override
    public Object getArray(Object field) {
        boolean[] inputValues = new boolean[0];
        if (field != null) {
            inputValues = Booleans.toArray((List<Boolean>) field);
        }
        return inputValues;
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
