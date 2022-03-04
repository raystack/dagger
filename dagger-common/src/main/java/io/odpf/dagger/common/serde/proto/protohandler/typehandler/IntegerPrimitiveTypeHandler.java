package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import com.google.common.primitives.Ints;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import io.odpf.dagger.common.serde.parquet.parser.validation.SimpleGroupValidation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.parquet.example.data.simple.SimpleGroup;

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
        if (field instanceof SimpleGroup) {
            return getValue((SimpleGroup) field);
        }
        return Integer.parseInt(getValueOrDefault(field, "0"));
    }

    private Object getValue(SimpleGroup simpleGroup) {
        String fieldName = fieldDescriptor.getName();

        /* this if branch checks that the field name exists in the simple group schema and is initialized */
        if (SimpleGroupValidation.checkFieldExistsAndIsInitialized(simpleGroup, fieldName)) {
            return simpleGroup.getInteger(fieldName, 0);
        } else {
            /* return default value */
            return 0;
        }
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
