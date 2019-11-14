package com.gojek.daggers.postProcessors;

import com.gojek.daggers.postProcessors.external.ExternalSourceConfig;
import com.gojek.daggers.postProcessors.external.es.EsSourceConfig;
import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;
import com.gojek.daggers.postProcessors.internal.InternalSourceConfig;
import com.gojek.daggers.postProcessors.transfromers.TransformConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.*;

public class ParentPostProcessorTest {

    @Test
    public void process() {
    }

    @Test
    public void shouldNotBeAbleToProcessWhenConfigIsNull() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null);

        Assert.assertFalse(parentPostProcessor.canProcess(null));
    }

    @Test
    public void shouldNotBeAbleToProcessWhenConfigIsEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null);
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>()),new ArrayList<>(), new ArrayList<>());

        Assert.assertFalse(parentPostProcessor.canProcess(postProcessorConfig));
    }

    @Test
    public void shouldBeAbleToProcessWhenEsConfigIsNotEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null);
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        es.add(new EsSourceConfig("","","","","","","","","","",false, new HashMap<>()));
        ExternalSourceConfig externalSource = new ExternalSourceConfig(new ArrayList<>(), es);
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(externalSource,new ArrayList<>(), new ArrayList<>());

        Assert.assertTrue(parentPostProcessor.canProcess(postProcessorConfig));
    }

    @Test
    public void shouldBeAbleToProcessWhenHttpConfigIsNotEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null);
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        http.add(new HttpSourceConfig("","","","","","",false,"","", new HashMap<>(), new HashMap<>()));
        ExternalSourceConfig externalSource = new ExternalSourceConfig(http, es);
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(externalSource,new ArrayList<>(), new ArrayList<>());

        Assert.assertTrue(parentPostProcessor.canProcess(postProcessorConfig));
    }

    @Test
    public void shouldBeAbleToProcessWhenInternalConfigIsNotEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null);
        ArrayList<InternalSourceConfig> internalSource = new ArrayList<>();
        internalSource.add(new InternalSourceConfig("outputField","value","type"));
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>()),new ArrayList<>(), internalSource);

        Assert.assertTrue(parentPostProcessor.canProcess(postProcessorConfig));
    }

    @Test
    public void shouldBeAbleToProcessWhenTransformConfigIsNotEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null);
        ArrayList<TransformConfig> transformers = new ArrayList<>();
        transformers.add(new TransformConfig("testClass", new HashMap<>()));
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>()), transformers, new ArrayList<>());

        Assert.assertTrue(parentPostProcessor.canProcess(postProcessorConfig));
    }
}