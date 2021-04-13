package io.odpf.dagger.processors.types;

import java.util.List;

public interface SourceConfig extends Validator {
    List<String> getOutputColumns();

    boolean isFailOnErrors();

    String getMetricId();

    String getPattern();

    String getVariables();

    String getType();
}
