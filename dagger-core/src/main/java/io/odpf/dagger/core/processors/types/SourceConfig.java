package io.odpf.dagger.core.processors.types;

import java.util.List;

/**
 * The interface Source config.
 */
public interface SourceConfig extends Validator {
    /**
     * Gets output columns.
     *
     * @return the output columns
     */
    List<String> getOutputColumns();

    /**
     * Check if the source config is failed on errors.
     *
     * @return the boolean
     */
    boolean isFailOnErrors();

    /**
     * Gets metric id.
     *
     * @return the metric id
     */
    String getMetricId();

    /**
     * Gets pattern.
     *
     * @return the pattern
     */
    String getPattern();

    /**
     * Gets variables.
     *
     * @return the variables
     */
    String getVariables();

    /**
     * Gets type.
     *
     * @return the type
     */
    String getType();
}
