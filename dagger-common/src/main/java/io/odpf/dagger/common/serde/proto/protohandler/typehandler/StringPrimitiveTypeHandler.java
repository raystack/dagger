package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * The type String primitive type handler.
 */
public class StringPrimitiveTypeHandler implements PrimitiveTypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new String primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public StringPrimitiveTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.STRING;
    }

    @Override
    public Object getValue(Object field) {
        return getValueOrDefault(field, "");
    }

    @Override
    public Object getArray(Object field) {
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
