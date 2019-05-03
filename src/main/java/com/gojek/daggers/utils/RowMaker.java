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

    public static Object fetchTypeAppropriateValue(Map<String, Object> inputMap, FieldDescriptor fieldDescriptor) {
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
                throw new RuntimeException("BYTE_STRING is not supported yet");
            case STRING:
                return getValueFor(inputMap, fieldDescriptor, "");
            case MESSAGE:
                if (fieldDescriptor.getMessageType().getFullName().equals("google.protobuf.Timestamp")) {
                    return inputMap.getOrDefault(fieldDescriptor.getName(), null);
                } else {
                    throw new RuntimeException("Complex Message types are not supported yet");
                }
            case ENUM:
                String input = inputMap.getOrDefault(fieldDescriptor.getName(), 0).toString();
                try {
                    int enumPosition = Integer.parseInt(input);
                    Descriptors.EnumValueDescriptor valueByNumber = fieldDescriptor.getEnumType().findValueByNumber(enumPosition);
                    return valueByNumber != null ? valueByNumber.getName() : fieldDescriptor.getEnumType().findValueByNumber(0).getName();
                } catch (NumberFormatException e) {
                    Descriptors.EnumValueDescriptor valueByName = fieldDescriptor.getEnumType().findValueByName(input);
                    return valueByName != null ? valueByName.getName() : fieldDescriptor.getEnumType().findValueByNumber(0).getName();
                }
            default:
                return null;
        }
    }

    private static String getValueFor(Map<String, Object> inputMap, FieldDescriptor fieldDescriptor, String defaultValue) {
        Object input = inputMap.get(fieldDescriptor.getName());
        return input == null ? defaultValue : input.toString();
    }
}
