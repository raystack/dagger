package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import com.google.common.primitives.Ints;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.List;

/**
 * The type Integer primitive type handler.
 */
public class IntegerPrimitiveTypeHandler implements PrimitiveTypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Integer primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public IntegerPrimitiveTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.INT;
    }

    @Override
    public Object getValue(Object field) {
        return Integer.parseInt(getValueOrDefault(field, "0"));
    }

    @Override
    public Object getArray(Object field) {
        int[] inputValues = new int[0];
        if (field != null) {
            inputValues = Ints.toArray((List<Integer>) field);
        }
        return inputValues;
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
