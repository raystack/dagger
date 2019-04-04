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
                return Integer.parseInt(getValueFor(inputMap, fieldDescriptor, "0"));
            case LONG:
                return Long.parseLong(getValueFor(inputMap, fieldDescriptor, "0"));
            case FLOAT:
                return Float.parseFloat(getValueFor(inputMap, fieldDescriptor, "0"));
            case DOUBLE:
                return Double.parseDouble(getValueFor(inputMap, fieldDescriptor, "0"));
            case BOOLEAN:
                return Boolean.parseBoolean(getValueFor(inputMap, fieldDescriptor, "false"));
            case BYTE_STRING:
                return new byte[0];
            case STRING:
                return getValueFor(inputMap, fieldDescriptor, "");
            case MESSAGE:
            case ENUM:
                return inputMap.getOrDefault(fieldDescriptor.getName(), null);
            default:
                return null;
        }
    }

    private static String getValueFor(Map<String, Object> inputMap, FieldDescriptor fieldDescriptor, String defaultValue) {
        Object input = inputMap.get(fieldDescriptor.getName());
        return input == null ? defaultValue : input.toString();
    }
}
