package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import io.odpf.dagger.common.serde.parquet.SimpleGroupValidation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * The type Long primitive type handler.
 */
public class LongPrimitiveTypeHandler implements PrimitiveTypeHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Long primitive type handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public LongPrimitiveTypeHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == JavaType.LONG;
    }

    @Override
    public Object getValue(Object field) {
        if (field instanceof SimpleGroup) {
            return getValue((SimpleGroup) field);
        }
        return Long.parseLong(getValueOrDefault(field, "0"));
    }

    private Object getValue(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();

        /* this if branch checks that the field name exists in the simple group schema and is initialized */
        if (SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            return simpleGroup.getLong(fieldName, 0);
        } else {
            /* return default value */
            return 0L;
        }
    }

    @Override
    public Object getArray(Object field) {
        List<Long> inputValues = new ArrayList<>();
        if (field != null) {
            inputValues = (List<Long>) field;
        }
        return inputValues.toArray(new Long[]{});
    }

    @Override
    public TypeInformation getTypeInformation() {
        return Types.LONG;
    }

    @Override
    public TypeInformation getArrayType() {
        return Types.OBJECT_ARRAY(Types.LONG);
    }
}
