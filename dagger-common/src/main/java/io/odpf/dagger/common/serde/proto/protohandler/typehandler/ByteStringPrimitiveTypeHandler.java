package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import java.util.ArrayList;
import java.util.List;

/**
 * The type Byte string primitive type handler.
 */
public class ByteStringPrimitiveTypeHandler implements PrimitiveTypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Byte string primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public ByteStringPrimitiveTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.BYTE_STRING;
    }

    @Override
    public Object getValue(Object field) {
        return field;
    }

    @Override
    public Object getArray(Object field) {
        List<ByteString> inputValues = new ArrayList<>();
        if (field != null) {
            inputValues = (List<ByteString>) field;
        }
        return inputValues.toArray(new ByteString[]{});
    }

    @Override
    public TypeInformation getTypeInformation() {
        return TypeInformation.of(ByteString.class);
    }

    @Override
    public TypeInformation getArrayType() {
        return Types.OBJECT_ARRAY(TypeInformation.of(ByteString.class));
    }
}
