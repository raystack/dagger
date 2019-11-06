package com.gojek.daggers.postprocessor.parser;

import com.gojek.daggers.postprocessor.parser.Validator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TransformConfig implements Serializable, Validator {

    private String transformationClass;
    private Map<String, String> transformationArguments;

    public TransformConfig(String transformationClass, Map<String, String> transformationArguments) {
        this.transformationClass = transformationClass;
        this.transformationArguments = transformationArguments;
    }

    public String getTransformationClass() {
        return transformationClass;
    }

    public Map<String, String> getTransformationArguments() {
        return transformationArguments;
    }

    public HashMap<String, Object> getMandatoryFields() {
        HashMap<String, Object> mandatoryFields = new HashMap<>();
        mandatoryFields.put("transformationClass", transformationClass);
        mandatoryFields.put("transformationArguments", transformationArguments);
        return mandatoryFields;
    }
}
