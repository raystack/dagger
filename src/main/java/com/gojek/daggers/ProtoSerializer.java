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
import java.util.Objects;

public class ProtoSerializer implements KeyedSerializationSchema<Row> {

    // TODO: Remove following property when migration to new portal is done
    private String protoClassNamePrefix;

    private String[] columnNames;
    private StencilClient stencilClient;

    private String keyProtoClassName;
    private String messageProtoClassName;

    // TODO: Remove following constructor when migration to new portal is done
    public ProtoSerializer(String protoClassNamePrefix, String[] columnNames, StencilClient stencilClient) {
        this.protoClassNamePrefix = protoClassNamePrefix;
        this.columnNames = columnNames;
        this.stencilClient = stencilClient;
    }

    public ProtoSerializer(String keyProtoClassName, String messageProtoClassName, String[] columnNames, StencilClient stencilClient) {
        if (Objects.isNull(messageProtoClassName)) {
            throw new DaggerProtoException("messageProtoClassName is required");
        }

        this.keyProtoClassName = keyProtoClassName;
        this.messageProtoClassName = messageProtoClassName;
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
        // TODO: Remove following block when migration to new portal is done
        if (!Objects.isNull(protoClassNamePrefix)) {
            return serialize(element, "Key");
        }

        if (Objects.isNull(keyProtoClassName)) {
            return null;
        }
        return parse(element, getDescriptor(keyProtoClassName)).toByteArray();
    }

    @Override
    public byte[] serializeValue(Row element) {
        // TODO: Remove following block when migration to new portal is done
        if (!Objects.isNull(protoClassNamePrefix)) {
            return serialize(element, "Message");
        }
        return parse(element, getDescriptor(messageProtoClassName)).toByteArray();
    }

    // TODO: Remove this method when migration to new protal is done
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
            try{
                builder = protoHandler.populate(builder, data);
            } catch (IllegalArgumentException e) {
                String errMessage = String.format("column invalid: type mismatch of column %s, expecting %s type", fieldDescriptor.getName(), fieldDescriptor.getType());
                throw new InvalidColumnMappingException(errMessage);
            }

        return builder;
    }

    @Override
    public String getTargetTopic(Row element) {
        return null;
    }
}
