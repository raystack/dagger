package com.gojek.daggers.protoHandler;

import com.gojek.daggers.exception.EnumFieldNotFoundException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

public class EnumProtoHandler implements ProtoHandler {
    private Descriptors.FieldDescriptor fieldDescriptor;

    public EnumProtoHandler(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public boolean canHandle() {
        return fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.ENUM;
    }

    @Override
    public DynamicMessage.Builder populateBuilder(DynamicMessage.Builder builder, Object field) {
        if (!canHandle() || field == null) {
            return builder;
        }
        String stringValue = String.valueOf(field).trim();
        Descriptors.EnumValueDescriptor valueByName = fieldDescriptor.getEnumType().findValueByName(stringValue);
        if (valueByName == null)
            throw new EnumFieldNotFoundException("field: " + stringValue + " not found in " + fieldDescriptor.getFullName());
        return builder.setField(fieldDescriptor, valueByName);
    }

    @Override
    public Object transform(Object field) {
        String input = field != null ? field.toString() : "0";
        try {
            int enumPosition = Integer.parseInt(input);
            Descriptors.EnumValueDescriptor valueByNumber = fieldDescriptor.getEnumType().findValueByNumber(enumPosition);
            return valueByNumber != null ? valueByNumber.getName() : fieldDescriptor.getEnumType().findValueByNumber(0).getName();
        } catch (NumberFormatException e) {
            Descriptors.EnumValueDescriptor valueByName = fieldDescriptor.getEnumType().findValueByName(input);
            return valueByName != null ? valueByName.getName() : fieldDescriptor.getEnumType().findValueByNumber(0).getName();
        }
    }
}
