package com.gojek.daggers.postprocessor.parser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class OutputMapping implements Serializable {

    private String path;
    private ArrayList fieldsMissing;

    public String getPath() {
        return path;
    }

    public void validate() throws IllegalArgumentException {
        HashMap<String, Object> mandatoryFields = new HashMap<>();
        mandatoryFields.put("path", path);
        fieldsMissing = new ArrayList<>();
        mandatoryFields.forEach((key, value) -> {
            if (value == null)
                fieldsMissing.add(key);
        });
        if (fieldsMissing.size() != 0)
            throw new IllegalArgumentException("Missing required fields: " + fieldsMissing.toString());
    }
}
