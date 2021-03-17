package com.gojek.daggers.processors.transformers;

import com.gojek.daggers.processors.types.Validator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TransformConfig implements Validator, Serializable {

    private final String transformationClass;
    private final Map<String, Object> transformationArguments;

    public TransformConfig(String transformationClass, Map<String, Object> transformationArguments) {
        this.transformationClass = transformationClass;
        this.transformationArguments = transformationArguments;
    }

    public TransformConfig() {
        this.transformationClass = "NULL";
        this.transformationArguments = new HashMap<>();
    }

    public String getTransformationClass() {
        return transformationClass;
    }

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
