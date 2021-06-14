package io.odpf.dagger.core.processors;

import io.odpf.dagger.core.processors.external.ExternalSourceConfig;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.transformers.TransformConfig;
import com.google.common.reflect.TypeToken;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.jayway.jsonpath.InvalidJsonException;
import io.odpf.dagger.core.utils.Constants;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * The Post processor config.
 */
public class PostProcessorConfig implements Serializable {

    private ExternalSourceConfig externalSource;
    private List<TransformConfig> transformers;
    private List<InternalSourceConfig> internalSource;
    private static final Gson GSON = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();

    /**
     * Instantiates a new Post processor config.
     *
     * @param externalSource the external source
     * @param transformers   the transformers
     * @param internalSource the internal source
     */
    public PostProcessorConfig(ExternalSourceConfig externalSource, List<TransformConfig> transformers, List<InternalSourceConfig> internalSource) {
        this.externalSource = externalSource;
        this.transformers = transformers;
        this.internalSource = internalSource;
    }

    /**
     * Parse post processor config.
     *
     * @param configuration the configuration
     * @return the post processor config
     */
    public static PostProcessorConfig parse(String configuration) {
        PostProcessorConfig postProcessorConfig;
        try {
            Type typeToken = new TypeToken<PostProcessorConfig>() {
            }.getType();
            postProcessorConfig = GSON.fromJson(configuration, typeToken);
        } catch (JsonSyntaxException exception) {
            throw new InvalidJsonException("Invalid JSON Given for " + Constants.PROCESSOR_POSTPROCESSOR_CONFIG_KEY);
        }

        return postProcessorConfig;
    }

    /**
     * Gets external source.
     *
     * @return the external source
     */
    public ExternalSourceConfig getExternalSource() {
        return externalSource;
    }

    /**
     * Gets internal source.
     *
     * @return the internal source
     */
    public List<InternalSourceConfig> getInternalSource() {
        return internalSource;
    }

    /**
     * Check if it has external source config.
     *
     * @return the boolean
     */
    public boolean hasExternalSource() {
        return externalSource != null && !externalSource.isEmpty();
    }

    /**
     * Check if it has internal source config.
     *
     * @return the boolean
     */
    public boolean hasInternalSource() {
        return internalSource != null && !internalSource.isEmpty();
    }

    /**
     * Check if transformers config, external source, and internal source is empty.
     *
     * @return the boolean
     */
    public boolean isEmpty() {
        return !hasTransformConfigs() && !hasExternalSource() && !hasInternalSource();
    }

    /**
     * Gets transformers.
     *
     * @return the transformers
     */
    public List<TransformConfig> getTransformers() {
        return transformers;
    }

    /**
     * Check if it has transformer configs.
     *
     * @return the boolean
     */
    public boolean hasTransformConfigs() {
        return transformers != null && !transformers.isEmpty();
    }

    /**
     * Check if it has sql transformer.
     *
     * @return the boolean
     */
    public boolean hasSQLTransformer() {
        return hasTransformConfigs() && transformers
                .stream()
                .anyMatch(transformConfig -> transformConfig
                        .getTransformationClass()
                        .equals(Constants.SQL_TRANSFORMER_CLASS));
    }

    /**
     * Gets output column names.
     *
     * @return the output column names
     */
    public List<String> getOutputColumnNames() {
        List<String> outputColumnNames = new ArrayList<>();
        if (externalSource != null && !externalSource.isEmpty()) {
            outputColumnNames.addAll(externalSource.getOutputColumnNames());
        }
        if (internalSource != null && !internalSource.isEmpty()) {
            internalSource.forEach(config -> outputColumnNames.add(config.getOutputField()));
        }
        return outputColumnNames;
    }
}
