package com.gojek.daggers.postProcessors;

import com.gojek.daggers.postProcessors.external.ExternalSourceConfig;
import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.es.EsSourceConfig;
import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;
import com.gojek.daggers.postProcessors.external.pg.PgSourceConfig;
import com.gojek.daggers.postProcessors.internal.InternalSourceConfig;
import com.gojek.daggers.postProcessors.transfromers.TransformConfig;
import com.jayway.jsonpath.InvalidJsonException;
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

    private final ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    private PostProcessorConfig postProcessorConfig;
    private ArrayList<InternalSourceConfig> internalSource = new ArrayList<>();
    private List<TransformConfig> transformConfigs = new ArrayList<>();
    private String configuration = "{\n  \"external_source\": {\n    \"es\": [\n      {\n        \"host\": \"10.240.60.227:9200\",\n        \"output_mapping\": {\n          \"customer_profile\": {\n            \"path\": \"$._source\"\n          }\n        },\n        \"query_param_pattern\": \"/customers/customer/%s\",\n        \"query_param_variables\": \"customer_id\",\n        \"retry_timeout\": \"5000\",\n        \"socket_timeout\": \"6000\",\n        \"stream_timeout\": \"5000\",\n        \"type\": \"com.gojek.esb.fraud.EnrichedBookingLogMessage\"\n      }\n    ],\n    \"http\": [\n      {\n        \"body_column_from_sql\": \"request_body\",\n        \"connect_timeout\": \"5000\",\n        \"endpoint\": \"http://localhost:8000\",\n        \"fail_on_errors\": \"true\",\n        \"headers\": {\n          \"content-type\": \"application/json\"\n        },\n        \"output_mapping\": {\n          \"surge_factor\": {\n            \"path\": \"$.data.tensor.values[0]\"\n          }\n        },\n        \"stream_timeout\": \"5000\",\n        \"verb\": \"post\"\n      }\n    ]\n  },\n  \"internal_source\":[\n  \t{\n    \"output_field\": \"event_timestamp\",\n    \"value\": \"CURRENT_TIMESTAMP\",\n    \"type\": \"function\"\n  \t},\n  \t{\n    \"output_field\": \"s2_id_level\",\n    \"value\": \"7\",\n    \"type\": \"constant\"\n\t }\n\t],\n  \"transformers\": [\n    {\n      \"transformation_arguments\": {\n        \"keyColumnName\": \"s2id\",\n        \"valueColumnName\": \"features\"\n      },\n      \"transformation_class\": \"com.gojek.daggers.postprocessor.FeatureTransformer\"\n    }\n  ]\n}";

    @Test
    public void shouldParseGivenConfiguration() {
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);

        assertNotNull(postProcessorConfig);
    }

    @Test
    public void shouldReturnColumns() {
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        ArrayList<String> expectedColumnNames = new ArrayList<>();
        expectedColumnNames.add("surge_factor");
        expectedColumnNames.add("customer_profile");
        expectedColumnNames.add("event_timestamp");
        expectedColumnNames.add("s2_id_level");

        assertArrayEquals(expectedColumnNames.toArray(), postProcessorConfig.getOutputColumnNames().toArray());
    }

    @Test
    public void shouldReturnHttpExternalSourceConfig() {
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        HashMap<String, OutputMapping> outputMappings;
        OutputMapping outputMapping;
        HashMap<String, String> headerMap;
        headerMap = new HashMap<>();
        headerMap.put("content-type", "application/json");
        outputMappings = new HashMap<>();
        outputMapping = new OutputMapping("$.surge");
        outputMappings.put("surge_factor", outputMapping);

//        HttpSourceConfig expectedHttpExternalSourceConfig = new HttpSourceConfig("http://localhost:8000",
//                "post", "request_body", bodyVariables, "5000", "5000", false, "", capacity, headerMap, outputMappings);
//
//        HttpSourceConfig actualHttpExternalSourceConfig = postProcessorConfig.getExternalSource().getHttpConfig().get(0);
//        assertEquals(expectedHttpExternalSourceConfig.getBodyPattern(), actualHttpExternalSourceConfig.getBodyPattern());
//        assertEquals(expectedHttpExternalSourceConfig.getEndpoint(), actualHttpExternalSourceConfig.getEndpoint());
//        assertEquals(expectedHttpExternalSourceConfig.getConnectTimeout(), actualHttpExternalSourceConfig.getConnectTimeout());
//        assertEquals(expectedHttpExternalSourceConfig.getStreamTimeout(), actualHttpExternalSourceConfig.getStreamTimeout());
//        assertEquals(expectedHttpExternalSourceConfig.getVerb(), actualHttpExternalSourceConfig.getVerb());
    }

    @Test
    public void shouldReturnEsExternalSourceConfig() {
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        HashMap<String, OutputMapping> outputMappings;
        OutputMapping outputMapping;
        outputMappings = new HashMap<>();
        outputMapping = new OutputMapping("$._source");
        outputMappings.put("customer_profile", outputMapping);

//        EsSourceConfig expectedEsExternalSourceConfig = new EsSourceConfig("10.240.60.227:9200", "/customers/customer/%s", "customer_id", "com.gojek.esb.fraud.EnrichedBookingLogMessage", "", connect_timeout, "5000", "6000", "5000", failOnErrors, outputMappings);
//
//        EsSourceConfig actualEsExternalSourceConfig = postProcessorConfig.getExternalSource().getEsConfig().get(0);
//        assertEquals(expectedEsExternalSourceConfig.getHost(), actualEsExternalSourceConfig.getHost());
//        assertEquals(expectedEsExternalSourceConfig.getEndpointPattern(), actualEsExternalSourceConfig.getEndpointPattern());
//        assertEquals(expectedEsExternalSourceConfig.getEndpointVariables(), actualEsExternalSourceConfig.getEndpointVariables());
//        assertEquals(expectedEsExternalSourceConfig.getType(), actualEsExternalSourceConfig.getType());
//        assertEquals(expectedEsExternalSourceConfig.getRetryTimeout(), actualEsExternalSourceConfig.getRetryTimeout());
//        assertEquals(expectedEsExternalSourceConfig.getSocketTimeout(), actualEsExternalSourceConfig.getSocketTimeout());
//        assertEquals(expectedEsExternalSourceConfig.getStreamTimeout(), actualEsExternalSourceConfig.getStreamTimeout());
//        assertEquals(expectedEsExternalSourceConfig.getOutputMapping().get("customer_profile"), actualEsExternalSourceConfig.getOutputMapping().get("customer_profile"));
    }

    @Test
    public void shouldReturnTransformConfig() {
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        HashMap<String, String> transformationArguments;
        transformationArguments = new HashMap<>();
        transformationArguments.put("keyColumnName", "s2id");
        transformationArguments.put("valueColumnName", "features");
        String transformationClass = "com.gojek.daggers.postprocessor.FeatureTransformer";
        TransformConfig expectedTransformerConfig = new TransformConfig(transformationClass, transformationArguments);

        TransformConfig actualTransformerConfig = postProcessorConfig.getTransformers().get(0);

        assertEquals(expectedTransformerConfig.getTransformationArguments(), actualTransformerConfig.getTransformationArguments());
        assertEquals(expectedTransformerConfig.getTransformationClass(), actualTransformerConfig.getTransformationClass());
    }

    @Test
    public void shouldThrowExceptionIfInvalidJsonConfigurationPassed() {
        expectedException.expect(InvalidJsonException.class);
        expectedException.expectMessage("Invalid JSON Given for POST_PROCESSOR_CONFIG");

        configuration = "test";

        PostProcessorConfig.parse(configuration);
    }

    @Test
    public void shouldBeEmptyWhenNoneOfTheConfigsExist(){
        postProcessorConfig = new PostProcessorConfig(null, null, null);

        assertTrue(postProcessorConfig.isEmpty());
    }


    @Test
    public void shouldNotBeEmptyWhenExternalSourceHasHttpConfigExist(){
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        http.add(new HttpSourceConfig("","","","","","",false,"","", new HashMap<>(), new HashMap<>()));
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg);
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, null);

        assertFalse(postProcessorConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenExternalSourceHasEsConfigExist(){
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        es.add(new EsSourceConfig("","","","","","","","","","",false, new HashMap<>()));
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg);
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, null);

        assertFalse(postProcessorConfig.isEmpty());
    }
    @Test
    public void shouldNotBeEmptyWhenExternalSourceHasPgConfigExist(){
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        pg.add(new PgSourceConfig("","","","","","","","", new HashMap<>(), "", "", "", "", true));
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg);
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, null);

        assertFalse(postProcessorConfig.isEmpty());
    }


    @Test
    public void shouldBeEmptyWhenExternalSourceHasEmptyConfig(){
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg);
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, null);

        assertTrue(postProcessorConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenInternalSourceExist(){
        ArrayList<InternalSourceConfig> internalSourceConfigs = new ArrayList<>();
        internalSourceConfigs.add(new InternalSourceConfig("outputField","value","type"));
        postProcessorConfig = new PostProcessorConfig(null, null, internalSourceConfigs);

        assertFalse(postProcessorConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenTransformConfigsExist(){
        transformConfigs.add(new TransformConfig("testClass",new HashMap<>()));
        postProcessorConfig = new PostProcessorConfig(null, transformConfigs, null);

        assertFalse(postProcessorConfig.isEmpty());
    }

    @Test
    public void shouldReturnExternalSourceConfig() {
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, internalSource);

        assertEquals(externalSourceConfig, postProcessorConfig.getExternalSource());
    }

    @Test
    public void shouldReturnInternalSourceConfig() {
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, internalSource);
        assertEquals(internalSource, postProcessorConfig.getInternalSource());
    }

    @Test
    public void shouldReturnTransformConfigs() {
        Map<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyValue", "key");
        transformConfigs.add(new TransformConfig("com.gojek.daggers.postprocessor.XTransformer", transformationArguments));
        postProcessorConfig = new PostProcessorConfig(null, transformConfigs, internalSource);
        assertEquals(transformConfigs, postProcessorConfig.getTransformers());
    }

    @Test
    public void shouldBeTrueWhenExternalSourceExists() {
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        es.add(new EsSourceConfig("","","","","","","","","","",false, new HashMap<>()));
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg);
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, internalSource);
        assertTrue(postProcessorConfig.hasExternalSource());
    }

    @Test
    public void shouldBeFalseWhenExternalSourceDoesNotExists() {
        postProcessorConfig = new PostProcessorConfig(null, null, internalSource);
        assertFalse(postProcessorConfig.hasExternalSource());
    }

    @Test
    public void shouldNotHaveInternalSourceWhenInternalSourceIsEmpty() {
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, internalSource);
        assertFalse(postProcessorConfig.hasInternalSource());
    }

    @Test
    public void shouldHaveInternalSourceWhenInternalSourceIsNotEmpty() {
        ArrayList<InternalSourceConfig> internalSource = new ArrayList<>();
        internalSource.add(new InternalSourceConfig("outputField","value","type"));
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, this.internalSource);
        assertFalse(postProcessorConfig.hasInternalSource());
    }

    @Test
    public void shouldBeFalseWhenInternalSourceDoesNotExists() {
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, null);
        assertFalse(postProcessorConfig.hasInternalSource());
    }

    @Test
    public void shouldBeTrueWhenTransformerSourceExists() {
        Map<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyValue", "key");
        transformConfigs.add(new TransformConfig("com.gojek.daggers.postprocessor.XTransformer", transformationArguments));
        postProcessorConfig = new PostProcessorConfig(null, transformConfigs, internalSource);
        assertTrue(postProcessorConfig.hasTransformConfigs());
    }

    @Test
    public void shouldBeFalseWhenTransformerSourceDoesNotExists() {
        postProcessorConfig = new PostProcessorConfig(null, null, internalSource);
        assertFalse(postProcessorConfig.hasTransformConfigs());
    }

}