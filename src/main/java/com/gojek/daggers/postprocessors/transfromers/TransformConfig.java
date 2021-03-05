package com.gojek.daggers.postprocessors.transfromers;

import com.gojek.daggers.postprocessors.common.Validator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TransformConfig implements Validator, Serializable {

    private String transformationClass;
    private Map<String, Object> transformationArguments;

    public TransformConfig(String transformationClass, Map<String, Object> transformationArguments) {
        this.transformationClass = transformationClass;
        this.transformationArguments = transformationArguments;
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
        mandatoryFields.put("transformationArguments", transformationArguments);
        return mandatoryFields;
    }
}
