package com.gojek.daggers;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class ProtoType implements Serializable {
    public static final String PROTO_CLASS_MISCONFIGURED_ERROR = "Proto class is misconfigured";
    private static final Map<JavaType, TypeInformation> TYPE_MAP = new HashMap<JavaType, TypeInformation>() {{
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

    private transient Descriptor protoFieldDescriptor;
    private String protoClassName;

    public ProtoType(String protoClassName) {
        this.protoClassName = protoClassName;
        this.protoFieldDescriptor = createFieldDescriptor();
    }

    private Descriptor getProtoFieldDescriptor() {
        if (protoFieldDescriptor == null) {
            protoFieldDescriptor = createFieldDescriptor();
        }
        return protoFieldDescriptor;
    }

    private Descriptor createFieldDescriptor() {
        try {
            Class<?> protoClass = Class.forName(protoClassName);
            return (Descriptor) protoClass.getMethod("getDescriptor").invoke(null);
        } catch (ReflectiveOperationException exception) {
            throw new DaggerConfigurationException(PROTO_CLASS_MISCONFIGURED_ERROR, exception);
        }
    }

    public String[] getFieldNames() {
        return getFieldNames(getProtoFieldDescriptor());
    }

    private String[] getFieldNames(Descriptor descriptor) {
        List<Descriptors.FieldDescriptor> fields = descriptor.getFields();

        return fields.stream()
                    .map(fieldDescriptor -> fieldDescriptor.getName()).toArray(String[]::new);
    }

    public TypeInformation[] getFieldTypes() {
        return getFieldTypes(getProtoFieldDescriptor());
    }

    private TypeInformation mapFieldType(Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.getJavaType() == JavaType.MESSAGE) {
            if (fieldDescriptor.isRepeated()) {
                return Types.OBJECT_ARRAY(getRowType(fieldDescriptor.getMessageType()));
            }
            return getRowType(fieldDescriptor.getMessageType());
        }
        if (fieldDescriptor.isRepeated()) {
            return Types.PRIMITIVE_ARRAY(TYPE_MAP.get(fieldDescriptor.getJavaType()));
        }
        return TYPE_MAP.get(fieldDescriptor.getJavaType());
    }

    private TypeInformation[] getFieldTypes(Descriptor descriptor) {
        return descriptor.getFields().stream()
            .map(this::mapFieldType).toArray(TypeInformation[]::new);
    }

    public TypeInformation<Row> getRowType() {
        return Types.ROW(getFieldNames(getProtoFieldDescriptor()), getFieldTypes(getProtoFieldDescriptor()));
    }

    private TypeInformation<Row> getRowType(Descriptor messageType) {
        return Types.ROW(getFieldNames(messageType), getFieldTypes(messageType));
    }
}
