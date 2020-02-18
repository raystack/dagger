package com.gojek.daggers.protoHandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
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
                row.setField(fieldDescriptor.getIndex(), protoHandler.transform(inputMap.get(fieldDescriptor.getName())));
            }
        }
        return row;
    }
}
