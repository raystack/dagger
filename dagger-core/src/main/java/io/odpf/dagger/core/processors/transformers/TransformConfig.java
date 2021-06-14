package io.odpf.dagger.core.processors.transformers;

import io.odpf.dagger.core.processors.types.Validator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A class that holds the Transformer configuration.
 */
public class TransformConfig implements Validator, Serializable {

    private final String transformationClass;
    private final Map<String, Object> transformationArguments;

    /**
     * Instantiates a new Transform config with specified transformation class and args.
     *
     * @param transformationClass     the transformation class
     * @param transformationArguments the transformation arguments
     */
    public TransformConfig(String transformationClass, Map<String, Object> transformationArguments) {
        this.transformationClass = transformationClass;
        this.transformationArguments = transformationArguments;
    }

    /**
     * Instantiates a new Transform config.
     */
    public TransformConfig() {
        this.transformationClass = "NULL";
        this.transformationArguments = new HashMap<>();
    }

    /**
     * Gets transformation class.
     *
     * @return the transformation class
     */
    public String getTransformationClass() {
        return transformationClass;
    }

    /**
     * Gets transformation arguments.
     *
     * @return the transformation arguments
     */
    public Map<String, Object> getTransformationArguments() {
        return transformationArguments;
    }

    public HashMap<String, Object> getMandatoryFields() {
        HashMap<String, Object> mandatoryFields = new HashMap<>();
        mandatoryFields.put("transformationClass", transformationClass);
        return mandatoryFields;
    }

    @Override
    public void validateFields() throws IllegalArgumentException {
        Validator.super.validateFields();
        for (TransformerUtils.DefaultArgument val : TransformerUtils.DefaultArgument.values()) {
            if (transformationArguments.containsKey(val.toString())) {
                throw new IllegalArgumentException("Transformation arguments cannot contain `" + val.toString() + "` as a key");
            }
        }
    }
}
