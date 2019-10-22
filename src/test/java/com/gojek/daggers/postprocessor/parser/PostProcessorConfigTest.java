package com.gojek.daggers.postprocessor.parser;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

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
        assertEquals(externalSource, postProcessorConfig.getExternalSource());
    }

    @Test
    public void shouldReturnTransformConfigs() {
        Map<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyValue", "key");
        transformConfigs.add(new TransformConfig("com.gojek.daggers.postprocessor.XTransformer", transformationArguments));
        postProcessorConfig = new PostProcessorConfig(null, transformConfigs);
        assertEquals(transformConfigs, postProcessorConfig.getTransformers());
    }

    @Test
    public void shouldBeTrueWhenExternalSourceExists(){
        externalSource.put("http", "test");
        postProcessorConfig = new PostProcessorConfig(externalSource, null);
        assertTrue(postProcessorConfig.hasExternalSource());
    }

    @Test
    public void shouldBeFalseWhenExternalSourceDoesNotExists(){
        postProcessorConfig = new PostProcessorConfig(null, null);
        assertFalse(postProcessorConfig.hasExternalSource());
    }

    @Test
    public void shouldBeTrueWhenTransformerSourceExists(){
        Map<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyValue", "key");
        transformConfigs.add(new TransformConfig("com.gojek.daggers.postprocessor.XTransformer", transformationArguments));
        postProcessorConfig = new PostProcessorConfig(null, transformConfigs);
        assertTrue(postProcessorConfig.hasTransformConfigs());
    }

    @Test
    public void shouldBeFalseWhenTransformerSourceDoesNotExists(){
        postProcessorConfig = new PostProcessorConfig(null, null);
        assertFalse(postProcessorConfig.hasTransformConfigs());
    }

}