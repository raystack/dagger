package com.gojek.daggers.postProcessors.external.common;

import com.gojek.daggers.postProcessors.common.Validator;

import java.util.List;

public interface SourceConfig extends Validator {
    List<String> getOutputColumns();


}
