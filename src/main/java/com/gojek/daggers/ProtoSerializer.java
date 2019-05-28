package com.gojek.daggers;

import com.gojek.daggers.exception.InvalidColumnMappingException;
import com.gojek.daggers.protoHandler.ProtoHandler;
import com.gojek.daggers.protoHandler.ProtoHandlerFactory;
import com.gojek.de.stencil.StencilClient;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class ProtoSerializer implements KeyedSerializationSchema<Row> {

    private String protoClassNamePrefix;
    private String[] columnNames;
    private StencilClient stencilClient;

    public ProtoSerializer(String protoClassNamePrefix, String[] columnNames, StencilClient stencilClient) {
        this.protoClassNamePrefix = protoClassNamePrefix;
        this.columnNames = columnNames;
        this.stencilClient = stencilClient;
    }

    private Descriptors.Descriptor getDescriptor(String className) {
        Descriptors.Descriptor dsc = stencilClient.get(className);
        if (dsc == null) {
            throw new DaggerProtoException();
        }
        return dsc;
    }

    @Override
    public byte[] serializeKey(Row element) {
        return serialize(element, "Key");
    }

    @Override
    public byte[] serializeValue(Row element) {
        return serialize(element, "Message");
    }

    private byte[] serialize(Row element, String suffix) {
        return parse(element, getDescriptor(protoClassNamePrefix + suffix)).toByteArray();
    }

    private DynamicMessage parse(Row element, Descriptors.Descriptor descriptor) {
        int numberOfElements = element.getArity();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (int index = 0; index < numberOfElements; index++) {
            String columnName = columnNames[index];
            Object data = element.getField(index);
            String[] nestedColumnNames = columnName.split("\\.");
            if (nestedColumnNames.length > 1) {
                Descriptors.FieldDescriptor firstField = descriptor.findFieldByName(nestedColumnNames[0]);
                if (firstField == null)
                    continue;
                builder = populateNestedBuilder(descriptor, nestedColumnNames, builder, data);
            } else {
                Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(columnName);
                builder = populateBuilder(builder, fieldDescriptor, data);
            }
        }
        return builder.build();
    }

    private DynamicMessage.Builder populateNestedBuilder(Descriptors.Descriptor parentDescriptor, String[] nestedColumnNames, DynamicMessage.Builder parentBuilder, Object data) {
        String childColumnName = nestedColumnNames[0];
        Descriptors.FieldDescriptor childFieldDescriptor = parentDescriptor.findFieldByName(childColumnName);
        if (childFieldDescriptor == null) {
            throw new InvalidColumnMappingException(String.format("column %s doesn't exists in the proto of %s", childColumnName, parentDescriptor.getFullName()));
        }
        if (nestedColumnNames.length == 1) {
            return populateBuilder(parentBuilder, childFieldDescriptor, data);
        }
        Descriptors.Descriptor childDescriptor = childFieldDescriptor.getMessageType();
        DynamicMessage.Builder childBuilder = DynamicMessage.newBuilder(childDescriptor);
        childBuilder.mergeFrom((DynamicMessage) parentBuilder.build().getField(childFieldDescriptor));
        parentBuilder.setField(childFieldDescriptor, populateNestedBuilder(childDescriptor, Arrays.copyOfRange(nestedColumnNames, 1, nestedColumnNames.length), childBuilder, data).build());
        return parentBuilder;
    }

    private DynamicMessage.Builder populateBuilder(DynamicMessage.Builder builder, Descriptors.FieldDescriptor fieldDescriptor, Object data) {
        if (fieldDescriptor == null) {
            return builder;
        }
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);
        if (data != null)
            builder = protoHandler.populate(builder, data);
        return builder;
    }

    @Override
    public String getTargetTopic(Row element) {
        return null;
    }
}
