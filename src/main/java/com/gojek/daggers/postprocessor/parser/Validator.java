package com.gojek.daggers.postprocessor.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public interface Validator {
    HashMap<String, Object> getMandatoryFields();

    default void validateFields() throws IllegalArgumentException {
        ArrayList fieldsMissing = new ArrayList<>();
        ArrayList<Object> nestedFields = new ArrayList<>();
        getMandatoryFields().forEach((key, value) -> {
            if (value == null)
                fieldsMissing.add(key);
            if (value instanceof Map)
                nestedFields.addAll(((Map) value).values());
        });
        if (fieldsMissing.size() != 0)
            throw new IllegalArgumentException("Missing required fields: " + fieldsMissing.toString());

        nestedFields.forEach(field -> {
            if (field instanceof Validator)
                ((Validator) field).validateFields();
        });
    }
}
