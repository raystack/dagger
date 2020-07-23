package com.gojek.daggers.protoHandler.typeHandler;

public interface PrimitiveTypeHandler {
    boolean canHandle();

    Object getValue(Object field);

    Object getArray(Object field);

    default String getValueOrDefault(Object input, String defaultValue) {
        return input == null ? defaultValue : input.toString();
    }
}
