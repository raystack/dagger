package com.gojek.daggers.postprocessor.parser;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostProcessorConfigTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private PostProcessorConfig postProcessorConfig;
    private Map<String, Object> externalSource = new HashMap<>();
    private List<TransformConfig> transformConfigs = new ArrayList<>();

    @Test
    public void shouldReturnPostProcessorConfig() {
        externalSource.put("http", "test");
        postProcessorConfig = new PostProcessorConfig(externalSource, null);
        Assert.assertEquals(externalSource, postProcessorConfig.getExternalSource());
    }

    @Test
    public void shouldReturnTransformConfigs() {
        Map<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyValue", "key");
        transformConfigs.add(new TransformConfig("com.gojek.daggers.postprocessor.XTransformer", transformationArguments));
        postProcessorConfig = new PostProcessorConfig(null, transformConfigs);
        Assert.assertEquals(transformConfigs, postProcessorConfig.getTransformers());
    }

}