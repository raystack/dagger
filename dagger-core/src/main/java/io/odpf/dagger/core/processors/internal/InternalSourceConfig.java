package io.odpf.dagger.core.processors.internal;

import io.odpf.dagger.core.processors.types.Validator;

import java.io.Serializable;
import java.util.HashMap;

public class InternalSourceConfig implements Validator, Serializable {

    private String outputField;
    private String value;
    private String type;

    public InternalSourceConfig(String outputField, String value, String type) {
        this.outputField = outputField;
        this.value = value;
        this.type = type;
    }

    @Override
    public HashMap<String, Object> getMandatoryFields() {
        HashMap<String, Object> mandatoryFields = new HashMap<>();
        mandatoryFields.put("output_field", outputField);
        mandatoryFields.put("type", type);
        mandatoryFields.put("value", value);

        return mandatoryFields;
    }

    public String getValue() {
        return value;
    }

    public String getType() {
        return type;
    }

    public String getOutputField() {
        return outputField;
    }
}
