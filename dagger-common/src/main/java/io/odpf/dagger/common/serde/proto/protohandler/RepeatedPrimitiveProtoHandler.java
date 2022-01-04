package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import io.odpf.dagger.common.serde.proto.protohandler.typehandler.PrimitiveTypeHandlerFactory;
import io.odpf.dagger.common.serde.proto.protohandler.typehandler.PrimitiveTypeHandler;
import com.google.gson.Gson;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.ENUM;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE;

/**
 * The type Repeated primitive proto handler.
 */
public class RepeatedPrimitiveProtoHandler implements ProtoHandler {
    private final FieldDescriptor fieldDescriptor;
    private static final Gson GSON = new Gson();

    /**
     * Instantiates a new Repeated primitive proto handler.
     *
     * @param fieldDescriptor the field descriptor
     */
    public RepeatedPrimitiveProtoHandler(FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.isRepeated() && fieldDescriptor.getJavaType() != MESSAGE && fieldDescriptor.getJavaType() != ENUM;
    }

    @Override
    public DynamicMessage.Builder transformForKafka(DynamicMessage.Builder builder, Object field) {
        if (!canHandle() || field == null) {
            return builder;
        }
        if (field.getClass().isArray()) {
            field = Arrays.asList((Object[]) field);
        }
        return builder.setField(fieldDescriptor, field);
    }

    @Override
    public Object transformFromPostProcessor(Object field) {
        ArrayList<Object> outputValues = new ArrayList<>();
        if (field != null) {
            List<Object> inputValues = (List<Object>) field;
            PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(fieldDescriptor);
            for (Object inputField : inputValues) {
                outputValues.add(primitiveProtoHandler.transformFromPostProcessor(inputField));
            }
        }
        return outputValues;
    }

    @Override
    public Object transformFromKafka(Object field) {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory.getTypeHandler(fieldDescriptor);
        return primitiveTypeHandler.getArray(field);
    }

    @Override
    public Object transformToJson(Object field) {
        return GSON.toJson(field);
    }

    @Override
    public TypeInformation getTypeInformation() {
        return PrimitiveTypeHandlerFactory.getTypeHandler(fieldDescriptor).getArrayType();
    }
}
