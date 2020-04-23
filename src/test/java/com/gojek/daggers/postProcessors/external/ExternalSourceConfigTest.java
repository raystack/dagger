package com.gojek.daggers.postProcessors.external;

import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.es.EsSourceConfig;
import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;
import com.gojek.daggers.postProcessors.external.pg.PgSourceConfig;
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
    private List<PgSourceConfig> pg;

    @Before
    public void setUp() throws Exception {
        HashMap<String, OutputMapping> httpOutputMapping = new HashMap<>();
        httpOutputMapping.put("http_field_1", new OutputMapping(""));
        httpOutputMapping.put("http_field_2", new OutputMapping(""));
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("endpoint", "POST", "/some/patttern/%s", "variable", "123", "234", false, "type", "20", new HashMap<>(), httpOutputMapping);
        http = new ArrayList<>();
        http.add(httpSourceConfig);
        es = new ArrayList<>();
        HashMap<String, OutputMapping> esOutputMapping = new HashMap<>();
        esOutputMapping.put("es_field_1", new OutputMapping(""));
        EsSourceConfig esSourceConfig = new EsSourceConfig("host", "port", "endpointPattern", "endpointVariable", "type", "30", "123", "234", "345", "456", false, esOutputMapping);
        es.add(esSourceConfig);
        pg = new ArrayList<>();
        HashMap<String, String> pgOutputMapping = new HashMap<>();
        pgOutputMapping.put("pg_field_1", "constant");
        PgSourceConfig pgSourceConfig = new PgSourceConfig("host", "port", "user", "password", "db", "type", "30", "123", pgOutputMapping, "123", "234", "345", "", "", true);
        pg.add(pgSourceConfig);
    }

    @Test
    public void shouldGetHttpConfig() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg);

        Assert.assertEquals(http, externalSourceConfig.getHttpConfig());
    }

    @Test
    public void shouldGetEsConfig() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg);

        Assert.assertEquals(es, externalSourceConfig.getEsConfig());
    }

    @Test
    public void shouldGetPgConfig() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg);

        Assert.assertEquals(pg, externalSourceConfig.getPgConfig());
    }

    @Test
    public void shouldBeEmptyWhenAllConfigsAreEmpty() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(null, null, null);

        Assert.assertTrue(externalSourceConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenEsConfigIsPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(null, es, null);

        Assert.assertFalse(externalSourceConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenHttpConfigIsPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, null, null);

        Assert.assertFalse(externalSourceConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenPgConfigIsPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(null, null, pg);

        Assert.assertFalse(externalSourceConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenHttpAndEsConfigIsPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, null);

        Assert.assertFalse(externalSourceConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenHttpAndPgConfigIsPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, null, pg);

        Assert.assertFalse(externalSourceConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenEsAndPgConfigIsPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(null, es, pg);

        Assert.assertFalse(externalSourceConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenAllConfigsArePresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg);

        Assert.assertFalse(externalSourceConfig.isEmpty());
    }

    @Test
    public void shouldGetOutputColumnNamesFromAllExternalSources() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg);

        List<String> columnNames = Arrays.asList("http_field_1", "http_field_2", "es_field_1", "pg_field_1");

        Assert.assertEquals(columnNames, externalSourceConfig.getOutputColumnNames());

    }


    @Test
    public void shouldGetOutputColumnNamesFromEsAndHttp() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, null);

        List<String> columnNames = Arrays.asList("http_field_1", "http_field_2", "es_field_1");

        Assert.assertEquals(columnNames, externalSourceConfig.getOutputColumnNames());

    }

    @Test
    public void shouldGetOutputColumnNamesFromPgAndHttp() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, null, pg);

        List<String> columnNames = Arrays.asList("http_field_1", "http_field_2", "pg_field_1");

        Assert.assertEquals(columnNames, externalSourceConfig.getOutputColumnNames());

    }

    @Test
    public void shouldGetOutputColumnNamesFromPgAndEs() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(null, es, pg);

        List<String> columnNames = Arrays.asList("es_field_1", "pg_field_1");

        Assert.assertEquals(columnNames, externalSourceConfig.getOutputColumnNames());

    }

        @Test
    public void shouldGetOutputColumnNamesFromEsWhenOthersAreNotPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(null, es, null);

        List<String> columnNames = Collections.singletonList("es_field_1");

        Assert.assertEquals(columnNames, externalSourceConfig.getOutputColumnNames());

    }

    @Test
    public void shouldGetOutputColumnNamesFromHttpWhenOthersAreNotPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, null, null);

        List<String> columnNames = Arrays.asList("http_field_1", "http_field_2");

        Assert.assertEquals(columnNames, externalSourceConfig.getOutputColumnNames());
    }

    @Test
    public void shouldGetOutputColumnNamesFromPgWhenOthersAreNotPresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(null, null, pg);

        List<String> columnNames = Arrays.asList("pg_field_1");

        Assert.assertEquals(columnNames, externalSourceConfig.getOutputColumnNames());
    }

    @Test
    public void shouldGetEmptyOutputColumnNamesWhenNonePresent() {
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(null, null, null);

        Assert.assertEquals(new ArrayList<>(), externalSourceConfig.getOutputColumnNames());
    }
}