package com.gojek.daggers;

import com.google.protobuf.Descriptors.Descriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class ProtoType {
    public static final String PROTO_CLASS_MISCONFIGURED_ERROR = "Proto class is misconfigured";
    private static final Map<JavaType, TypeInformation> TYPE_MAP = new HashMap<JavaType, TypeInformation>(){{
        put(JavaType.STRING, Types.STRING());
        put(JavaType.BOOLEAN, Types.BOOLEAN());
        put(JavaType.DOUBLE, Types.DOUBLE());
        put(JavaType.LONG, Types.LONG());
        put(JavaType.MESSAGE, Types.ROW());
        put(JavaType.ENUM, Types.STRING());
        put(JavaType.INT, Types.INT());
        put(JavaType.FLOAT, Types.FLOAT());
        put(JavaType.BYTE_STRING, Types.STRING());
    }};

    private Descriptor protoFieldDescriptor;

    public ProtoType(String protoClassName) {
        try {
            Class<?> protoClass = Class.forName(protoClassName);
            protoFieldDescriptor = (Descriptor) protoClass.getMethod("getDescriptor").invoke(null);
        } catch (ReflectiveOperationException exception) {
            throw new DaggerConfigurationException(PROTO_CLASS_MISCONFIGURED_ERROR, exception);
        }
    }

    public String[] getFieldNames() {
        return getFieldNames(protoFieldDescriptor);
    }

    private String[] getFieldNames(Descriptor descriptor) {
        return descriptor.getFields().stream().map(fieldDescriptor -> fieldDescriptor.getName()).toArray(String[]::new);
    }

    public TypeInformation[] getFieldTypes() {
        return getFieldTypes(protoFieldDescriptor);
    }

    private TypeInformation[] getFieldTypes(Descriptor descriptor) {
        return descriptor.getFields().stream()
                .map(fieldDescriptor -> {
                    if(fieldDescriptor.getJavaType() == JavaType.MESSAGE){
                        return getRowType(fieldDescriptor.getMessageType());
                    }
                    return TYPE_MAP.get(fieldDescriptor.getJavaType());
                }).toArray(TypeInformation[]::new);
    }

    private TypeInformation<Row> getRowType(Descriptor messageType) {
        return Types.ROW(getFieldNames(messageType), getFieldTypes(messageType));
    }
}
