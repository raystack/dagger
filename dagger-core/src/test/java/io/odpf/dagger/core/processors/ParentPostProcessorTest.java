package io.odpf.dagger.core.processors;

import io.odpf.dagger.core.metrics.telemetry.TelemetrySubscriber;
import io.odpf.dagger.core.processors.external.ExternalSourceConfig;
import io.odpf.dagger.core.processors.external.es.EsSourceConfig;
import io.odpf.dagger.core.processors.external.http.HttpSourceConfig;
import io.odpf.dagger.core.processors.external.pg.PgSourceConfig;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.transformers.TransformConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.HashMap;

public class ParentPostProcessorTest {

    @Mock
    private TelemetrySubscriber telemetrySubscriber;

    @Test
    public void shouldNotBeAbleToProcessWhenConfigIsNull() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null, telemetrySubscriber);

        Assert.assertFalse(parentPostProcessor.canProcess(null));
    }

    @Test
    public void shouldNotBeAbleToProcessWhenConfigIsEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null, telemetrySubscriber);
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>()), new ArrayList<>(), new ArrayList<>());

        Assert.assertFalse(parentPostProcessor.canProcess(postProcessorConfig));
    }

    @Test
    public void shouldBeAbleToProcessWhenEsConfigIsNotEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null, telemetrySubscriber);
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        es.add(new EsSourceConfig("", "", "", "", "", "", "", "", "", "", "", "", false, new HashMap<>(), "metricId_01", false));
        ExternalSourceConfig externalSource = new ExternalSourceConfig(new ArrayList<>(), es, new ArrayList<>(), new ArrayList<>());
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(externalSource, new ArrayList<>(), new ArrayList<>());

        Assert.assertTrue(parentPostProcessor.canProcess(postProcessorConfig));
    }

    @Test
    public void shouldBeAbleToProcessWhenHttpConfigIsNotEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null, telemetrySubscriber);
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        http.add(new HttpSourceConfig("", "", "", "", "", "", false, "", "", new HashMap<>(), new HashMap<>(), "metricId_01", false));
        ExternalSourceConfig externalSource = new ExternalSourceConfig(http, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(externalSource, new ArrayList<>(), new ArrayList<>());

        Assert.assertTrue(parentPostProcessor.canProcess(postProcessorConfig));
    }

    @Test
    public void shouldBeAbleToProcessWhenPgConfigIsNotEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null, telemetrySubscriber);
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        pg.add(new PgSourceConfig("", "", "", "", "", "", "", "", new HashMap<>(), "", "", "", "", true, "metricId_01", false));
        ExternalSourceConfig externalSource = new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>(), pg, new ArrayList<>());
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(externalSource, new ArrayList<>(), new ArrayList<>());

        Assert.assertTrue(parentPostProcessor.canProcess(postProcessorConfig));
    }

    @Test
    public void shouldBeAbleToProcessWhenInternalConfigIsNotEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null, telemetrySubscriber);
        ArrayList<InternalSourceConfig> internalSource = new ArrayList<>();
        internalSource.add(new InternalSourceConfig("outputField", "value", "type"));
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>()), new ArrayList<>(), internalSource);

        Assert.assertTrue(parentPostProcessor.canProcess(postProcessorConfig));
    }

    @Test
    public void shouldBeAbleToProcessWhenTransformConfigIsNotEmpty() {
        ParentPostProcessor parentPostProcessor = new ParentPostProcessor(null, null, null, telemetrySubscriber);
        ArrayList<TransformConfig> transformers = new ArrayList<>();
        transformers.add(new TransformConfig("testClass", new HashMap<>()));
        PostProcessorConfig postProcessorConfig = new PostProcessorConfig(new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>()), transformers, new ArrayList<>());

        Assert.assertTrue(parentPostProcessor.canProcess(postProcessorConfig));
    }
}
