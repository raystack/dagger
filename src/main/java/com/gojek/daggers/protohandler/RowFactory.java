package com.gojek.daggers.protohandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

public class RowFactory {
    public static Row createRow(Map<String, Object> inputMap, Descriptors.Descriptor descriptor) {
        List<FieldDescriptor> descriptorFields = descriptor.getFields();
        Row row = new Row(descriptorFields.size());
        if (inputMap == null)
            return row;
        for (FieldDescriptor fieldDescriptor : descriptorFields) {
            ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);
            if (inputMap.get(fieldDescriptor.getName()) != null) {
                row.setField(fieldDescriptor.getIndex(), protoHandler.transformFromPostProcessor(inputMap.get(fieldDescriptor.getName())));
            }
        }
        return row;
    }

    public static Row createRow(DynamicMessage proto, int extraColumns) {
        List<FieldDescriptor> descriptorFields = proto.getDescriptorForType().getFields();
        Row row = new Row(descriptorFields.size() + extraColumns);
        for (FieldDescriptor fieldDescriptor : descriptorFields) {
            ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);
            row.setField(fieldDescriptor.getIndex(), protoHandler.transformFromKafka(proto.getField(fieldDescriptor)));
        }
        return row;
    }

    public static Row createRow(DynamicMessage proto) {
        return createRow(proto, 0);
    }
}
