package com.gojek.daggers.postprocessors.external.common;

import com.gojek.daggers.postprocessors.common.Validator;

import java.util.List;

public interface SourceConfig extends Validator {
    List<String> getOutputColumns();

    boolean isFailOnErrors();

    String getMetricId();

    String getPattern();

    String getVariables();

    String getType();
}
