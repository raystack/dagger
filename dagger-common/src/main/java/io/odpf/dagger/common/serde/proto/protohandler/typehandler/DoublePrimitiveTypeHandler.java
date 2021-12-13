package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import com.google.common.primitives.Doubles;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.List;

/**
 * The type Double primitive type handler.
 */
public class DoublePrimitiveTypeHandler implements PrimitiveTypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Double primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public DoublePrimitiveTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.DOUBLE;
    }

    @Override
    public Object getValue(Object field) {
        return Double.parseDouble(getValueOrDefault(field, "0"));
    }

    @Override
    public Object getArray(Object field) {
        double[] inputValues = new double[0];
        if (field != null) {
            inputValues = Doubles.toArray((List<Double>) field);
        }
        return inputValues;
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.DOUBLE;
    }

    @Override
    public TypeInformation getArrayType() {
        return Types.PRIMITIVE_ARRAY(Types.DOUBLE);
    }
}
