package com.gojek.daggers.postProcessors.external.common;

import com.gojek.daggers.postProcessors.common.Validator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;

public class OutputMapping implements Serializable, Validator {

    private String path;

    public OutputMapping(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public HashMap<String, Object> getMandatoryFields() {
        HashMap<String, Object> mandatoryFields = new HashMap<>();
        mandatoryFields.put("path", path);
        return mandatoryFields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutputMapping that = (OutputMapping) o;
        return Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }
}
