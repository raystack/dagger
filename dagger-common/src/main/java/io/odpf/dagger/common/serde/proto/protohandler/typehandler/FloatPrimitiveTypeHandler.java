package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import com.google.common.primitives.Floats;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.List;

/**
 * The type Float primitive type handler.
 */
public class FloatPrimitiveTypeHandler implements PrimitiveTypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Float primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public FloatPrimitiveTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.FLOAT;
    }

    @Override
    public Object getValue(Object field) {
        return Float.parseFloat(getValueOrDefault(field, "0"));
    }

    @Override
    public Object getArray(Object field) {

        float[] inputValues = new float[0];
        if (field != null) {
            inputValues = Floats.toArray((List<Float>) field);
        }
        return inputValues;
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
