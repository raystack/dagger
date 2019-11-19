package com.gojek.daggers.postProcessors.common;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;

// TODO Need for Refactoring this class
public class RowMaker {
    public static Row makeRow(Map<String, Object> inputMap, Descriptors.Descriptor descriptor) {
        List<FieldDescriptor> descriptorFields = descriptor.getFields();
        Row row = new Row(descriptorFields.size());
        for (FieldDescriptor fieldDescriptor : descriptorFields) {
            row.setField(fieldDescriptor.getIndex(), fetchTypeAppropriateValue(inputMap.get(fieldDescriptor.getName()), fieldDescriptor));
        }
        return row;
    }

    public static Object fetchTypeAppropriateValue(Object field, FieldDescriptor fieldDescriptor) {
        switch (fieldDescriptor.getJavaType()) {
            case INT:
                return Integer.parseInt(getValueFor(field, "0"));
            case LONG:
                return Long.parseLong(getValueFor(field, "0"));
            case FLOAT:
                return Float.parseFloat(getValueFor(field, "0"));
            case DOUBLE:
                return Double.parseDouble(getValueFor(field, "0"));
            case BOOLEAN:
                return Boolean.parseBoolean(getValueFor(field, "false"));
            case BYTE_STRING:
                throw new RuntimeException("BYTE_STRING is not supported yet");
            case STRING:
                return getValueFor(field, "");
            case MESSAGE:
                if (fieldDescriptor.getMessageType().getFullName().equals("google.protobuf.Timestamp")) {
                    Object inputTimeStamp = field;
                    if (inputTimeStamp == null) {
                        return null;
                    }
                    try {
                        Instant.parse(inputTimeStamp.toString());
                    } catch (DateTimeParseException e) {
                        return null;
                    }
                    return inputTimeStamp.toString();
                } else {
                    return null;
                    //TODO not handling complex types
                }
            case ENUM:
                String input = field != null ? field.toString() : "0";
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

    private static String getValueFor(Object input, String defaultValue) {
        return input == null ? defaultValue : input.toString();
    }
}
