package io.odpf.dagger.core.processors.internal;

import io.odpf.dagger.core.processors.types.Validator;

import java.io.Serializable;
import java.util.HashMap;

/**
 * A class that holds internal post processor configuration.
 */
public class InternalSourceConfig implements Validator, Serializable {

    private String outputField;
    private String value;
    private String type;

    /**
     * Instantiates a new Internal source config.
     *
     * @param outputField the output field
     * @param value       the value
     * @param type        the type
     */
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

    /**
     * Gets value.
     *
     * @return the value
     */
    public String getValue() {
        return value;
    }

    /**
     * Gets type.
     *
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * Gets output field.
     *
     * @return the output field
     */
    public String getOutputField() {
        return outputField;
    }
}
