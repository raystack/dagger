package com.gojek.daggers.utils;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

public class RowMaker {
    public static Row makeRow(Map<String, Object> inputMap, Descriptors.Descriptor descriptor) {
        List<FieldDescriptor> descriptorFields = descriptor.getFields();
        Row row = new Row(descriptorFields.size());
        for (FieldDescriptor fieldDescriptor : descriptorFields) {
            row.setField(fieldDescriptor.getIndex(), fetchTypeAppropriateValue(inputMap, fieldDescriptor));
        }
        return row;
    }

    private static Object fetchTypeAppropriateValue(Map<String, Object> inputMap, FieldDescriptor fieldDescriptor) {
        switch (fieldDescriptor.getJavaType()) {
            case INT:
                return Integer.parseInt(inputMap.getOrDefault(fieldDescriptor.getName(), "0").toString());
            case LONG:
                return Long.parseLong(inputMap.getOrDefault(fieldDescriptor.getName(), "0").toString());
            case FLOAT:
                return Float.parseFloat(inputMap.getOrDefault(fieldDescriptor.getName(), "0").toString());
            case DOUBLE:
                return Double.parseDouble(inputMap.getOrDefault(fieldDescriptor.getName(), "0").toString());
            case BOOLEAN:
                return Boolean.parseBoolean(inputMap.getOrDefault(fieldDescriptor.getName(), "").toString());
            case BYTE_STRING:
                return new byte[0];
            case STRING:
                return inputMap.getOrDefault(fieldDescriptor.getName(), "").toString();
            case MESSAGE:
            case ENUM:
                return inputMap.getOrDefault(fieldDescriptor.getName(), null);
            default:
                return null;
        }
    }
}
