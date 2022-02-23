package io.odpf.dagger.core.processors.internal;

import io.odpf.dagger.core.processors.types.Validator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A class that holds internal post processor configuration.
 */
public class InternalSourceConfig implements Validator, Serializable {
    public static final String INTERNAL_PROCESSOR_CONFIG_KEY = "internal_processor_config";

    private String outputField;
    private String value;
    private String type;
    private Map<String, String> internalProcessorConfig;

    /**
     * Instantiates a new Internal source config.
     *
     * @param outputField             the output field
     * @param value                   the value
     * @param type                    the type
     * @param internalProcessorConfig the internal processor config
     */
    public InternalSourceConfig(String outputField, String value, String type, Map<String, String> internalProcessorConfig) {
        this.outputField = outputField;
        this.value = value;
        this.type = type;
        this.internalProcessorConfig = internalProcessorConfig;
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

    /**
     * Gets internal processor config.
     *
     * @return the internal processor config
     */
    public Map<String, String> getInternalProcessorConfig() {
        return internalProcessorConfig;
    }
}
