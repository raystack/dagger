package com.gojek.daggers.protoHandler.typeHandler;

public interface TypeHandler {
    boolean canHandle();

    Object getValue(Object field);

    default String getValueOrDefault(Object input, String defaultValue) {
        return input == null ? defaultValue : input.toString();
    }
}
