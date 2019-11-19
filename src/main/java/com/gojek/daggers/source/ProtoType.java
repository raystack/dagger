package com.gojek.daggers.source;

import com.gojek.daggers.exception.DaggerProtoException;
import com.gojek.de.stencil.StencilClient;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class ProtoType implements Serializable {
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
    private String rowtimeAttributeName;
    private StencilClient stencilClient;

    public ProtoType(String protoClassName, String rowtimeAttributeName, StencilClient stencilClient) {
        this.stencilClient = stencilClient;
        this.protoClassName = protoClassName;
        this.protoFieldDescriptor = createFieldDescriptor();
        this.rowtimeAttributeName = rowtimeAttributeName;
    }

    private Descriptor getProtoFieldDescriptor() {
        if (protoFieldDescriptor == null) {
            protoFieldDescriptor = createFieldDescriptor();
        }
        return protoFieldDescriptor;
    }

    private Descriptor createFieldDescriptor() {
        Descriptors.Descriptor dsc = stencilClient.get(protoClassName);
        if (dsc == null) {
            throw new DaggerProtoException();
        }
        return dsc;
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
            if (fieldDescriptor.toProto().getTypeName().equals(".google.protobuf.Struct")) {
                if (fieldDescriptor.isRepeated()) {
                    return Types.OBJECT_ARRAY(getRowTypeForStruct(fieldDescriptor.getMessageType()));
                }
                return getRowTypeForStruct(fieldDescriptor.getMessageType());
            }
            if (fieldDescriptor.isRepeated()) {
                return Types.OBJECT_ARRAY(getRowType(fieldDescriptor.getMessageType()));
            }
            return getRowType(fieldDescriptor.getMessageType());
        }
        if (fieldDescriptor.isRepeated()) {
            if (fieldDescriptor.getJavaType() == JavaType.STRING) {
                return Types.OBJECT_ARRAY(TYPE_MAP.get(fieldDescriptor.getJavaType()));
            }
            return Types.PRIMITIVE_ARRAY(TYPE_MAP.get(fieldDescriptor.getJavaType()));
        }
        return TYPE_MAP.get(fieldDescriptor.getJavaType());
    }

    private TypeInformation[] getFieldTypes(Descriptor descriptor) {
        return descriptor.getFields().stream()
                .map(this::mapFieldType).toArray(TypeInformation[]::new);
    }

    public TypeInformation<Row> getRowType() {
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<TypeInformation> fieldTypes = new ArrayList<>();
        fieldNames.addAll(Arrays.asList(getFieldNames(getProtoFieldDescriptor())));
        fieldTypes.addAll(Arrays.asList(getFieldTypes(getProtoFieldDescriptor())));
        fieldNames.add(rowtimeAttributeName);
        fieldTypes.add(org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP);
        TypeInformation<Row> rowTypeInformation = org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(fieldNames.toArray(new String[0]), fieldTypes.toArray(new TypeInformation[0]));


        return rowTypeInformation;
    }

    private TypeInformation<Row> getRowType(Descriptor messageType) {
        return Types.ROW(getFieldNames(messageType), getFieldTypes(messageType));
    }

    private TypeInformation<Row> getRowTypeForStruct(Descriptor messageType) {
        List<Descriptors.FieldDescriptor> fields = messageType.getFields();
        String[] names = fields
                .stream()
                .filter(fieldDescriptor -> fieldDescriptor.toProto().getTypeName().equals(".google.protobuf.Struct"))
                .map(fieldDescriptor -> fieldDescriptor.getName())
                .toArray(String[]::new);
        TypeInformation[] types = fields
                .stream()
                .filter(fieldDescriptor -> fieldDescriptor.toProto().getTypeName().equals(".google.protobuf.Struct"))
                .map(this::mapFieldType)
                .toArray(TypeInformation[]::new);
        return Types.ROW(names, types);
    }
}
