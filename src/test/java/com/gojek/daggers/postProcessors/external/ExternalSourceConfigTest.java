package com.gojek.daggers.postProcessors.external;

import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.es.EsSourceConfig;
import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class ExternalSourceConfigTest {

    private ArrayList<HttpSourceConfig> http;
    private ArrayList<EsSourceConfig> es;
    private HashMap<String, OutputMapping> esOutputMapping;
    private HashMap<String, OutputMapping> httpColumnNames;

    @Before
    public void setUp() throws Exception {
        httpColumnNames = new HashMap<>();
        httpColumnNames.put("http_field_1", new OutputMapping(""));
        httpColumnNames.put("http_field_2", new OutputMapping(""));
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("endpoint", "POST", "/some/patttern/%s", "variable", "123", "234", false, "type", "20", new HashMap<>(), httpColumnNames);
        http = new ArrayList<>();
        http.add(httpSourceConfig);
        es = new ArrayList<>();
        esOutputMapping = new HashMap<>();
        esOutputMapping.put("es_field_1", new OutputMapping(""));
        EsSourceConfig esSourceConfig = new EsSourceConfig("host", "port", "endpointPattern", "endpointVariable", "type", "30", "123", "234", "345", "456", false, esOutputMapping);
        es.add(esSourceConfig);
    }

    @Test
    public void shouldGetHttpConfig() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es);

        Assert.assertEquals(http, externalSourceConfig.getHttpConfig());
    }

    @Test
    public void shouldGetEsConfig() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es);

        Assert.assertEquals(es, externalSourceConfig.getEsConfig());
    }

    @Test
    public void shouldBeEmptyWhenBothConfigIsEmpty() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(null, null);

        Assert.assertTrue(externalSourceConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenEsConfigIsPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(null, es);

        Assert.assertFalse(externalSourceConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenHttpConfigIsPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, null);

        Assert.assertFalse(externalSourceConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenHttpAndEsConfigIsPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es);

        Assert.assertFalse(externalSourceConfig.isEmpty());
    }

    @Test
    public void shouldGetOutputColumnNamesFromEsAndHttp() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es);

        List<String> columnNames = Arrays.asList("http_field_1", "http_field_2", "es_field_1");

        Assert.assertEquals(columnNames, externalSourceConfig.getOutputColumnNames());

    }

    @Test
    public void shouldGetOutputColumnNamesFromEsWhenHttpIsNotPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(null, es);

        List<String> columnNames = Collections.singletonList("es_field_1");

        Assert.assertEquals(columnNames, externalSourceConfig.getOutputColumnNames());

    }

    @Test
    public void shouldGetOutputColumnNamesFromHttpWhenEsIsNotPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, null);

        List<String> columnNames = Arrays.asList("http_field_1", "http_field_2");

        Assert.assertEquals(columnNames, externalSourceConfig.getOutputColumnNames());
    }

    @Test
    public void shouldGetEmptyOutputColumnNamesFromHttpWhenEsIsNotPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(null, null);

        Assert.assertEquals(new ArrayList<>(), externalSourceConfig.getOutputColumnNames());
    }
}