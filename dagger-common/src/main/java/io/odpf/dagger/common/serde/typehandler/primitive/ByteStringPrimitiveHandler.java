package io.odpf.dagger.common.serde.typehandler.primitive;

import io.odpf.dagger.common.serde.parquet.SimpleGroupValidation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * The type Byte string primitive type handler.
 */
public class ByteStringPrimitiveHandler implements PrimitiveHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Byte string primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public ByteStringPrimitiveHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.BYTE_STRING;
    }

    @Override
    public Object parseObject(Object field) {
        return field;
    }

    @Override
    public Object parseSimpleGroup(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();

        /* this if branch checks that the field name exists in the simple group schema and is initialized */
        if (SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            byte[] byteArray = simpleGroup.getBinary(fieldName, 0).getBytes();
            return ByteString.copyFrom(byteArray);
        } else {
            return null;
        }
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
