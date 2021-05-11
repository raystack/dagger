package io.odpf.dagger.core.protohandler.typehandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.ArrayList;
import java.util.List;

public class IntegerPrimitiveTypeHandler implements PrimitiveTypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

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
        List<Integer> inputValues = new ArrayList<>();
        if (field != null) {
            inputValues = (List<Integer>) field;
        }
        return inputValues.toArray(new Integer[]{});
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
