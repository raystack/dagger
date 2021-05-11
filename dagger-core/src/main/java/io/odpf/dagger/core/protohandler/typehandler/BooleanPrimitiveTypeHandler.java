package io.odpf.dagger.core.protohandler.typehandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.ArrayList;
import java.util.List;

public class BooleanPrimitiveTypeHandler implements PrimitiveTypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

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
        List<Boolean> inputValues = new ArrayList<>();
        if (field != null) {
            inputValues = (List<Boolean>) field;
        }
        return inputValues.toArray(new Boolean[]{});
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
