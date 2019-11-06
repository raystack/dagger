package com.gojek.daggers.postprocessor.parser;

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

    private final ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>());
    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    private PostProcessorConfig postProcessorConfig;
    private List<TransformConfig> transformConfigs = new ArrayList<>();
    private String configuration = "{\"external_source\":{\"es\":[{\"host\":\"10.240.60.227:9200\",\"output_mapping\":{\"customer_profile\":{\"path\":\"$._source\"}},\"query_param_pattern\":\"/customers/customer/%s\",\"query_param_variables\":\"customer_id\",\"retry_timeout\":\"5000\",\"socket_timeout\":\"6000\",\"stream_timeout\":\"5000\",\"type\":\"com.gojek.esb.fraud.EnrichedBookingLogMessage\"}],\"http\":[{\"body_column_from_sql\":\"request_body\",\"connect_timeout\":\"5000\",\"endpoint\":\"http://localhost:8000\",\"fail_on_errors\":\"true\",\"headers\":{\"content-type\":\"application/json\"},\"output_mapping\":{\"surge_factor\":{\"path\":\"$.data.tensor.values[0]\"}},\"stream_timeout\":\"5000\",\"verb\":\"post\"}]},\"transformers\":[{\"transformation_arguments\":{\"keyColumnName\":\"s2id\",\"valueColumnName\":\"features\"},\"transformation_class\":\"com.gojek.daggers.postprocessor.FeatureTransformer\"}]}";

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
        assertArrayEquals(expectedColumnNames.toArray(), postProcessorConfig.getColumns().toArray());
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

        HttpExternalSourceConfig expectedHttpExternalSourceConfig = new HttpExternalSourceConfig("http://localhost:8000",
                "post", "request_body", "5000", "5000", false, "", headerMap, outputMappings);

        HttpExternalSourceConfig actualHttpExternalSourceConfig = postProcessorConfig.getExternalSource().getHttpConfig().get(0);
        assertEquals(expectedHttpExternalSourceConfig.getBodyColumnFromSql(), actualHttpExternalSourceConfig.getBodyColumnFromSql());
        assertEquals(expectedHttpExternalSourceConfig.getEndpoint(), actualHttpExternalSourceConfig.getEndpoint());
        assertEquals(expectedHttpExternalSourceConfig.getConnectTimeout(), actualHttpExternalSourceConfig.getConnectTimeout());
        assertEquals(expectedHttpExternalSourceConfig.getStreamTimeout(), actualHttpExternalSourceConfig.getStreamTimeout());
        assertEquals(expectedHttpExternalSourceConfig.getVerb(), actualHttpExternalSourceConfig.getVerb());
    }

    @Test
    public void shouldReturnEsExternalSourceConfig() {
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        HashMap<String, OutputMapping> outputMappings;
        OutputMapping outputMapping;
        outputMappings = new HashMap<>();
        outputMapping = new OutputMapping("$._source");
        outputMappings.put("customer_profile", outputMapping);

        EsExternalSourceConfig expectedEsExternalSourceConfig = new EsExternalSourceConfig("10.240.60.227:9200", "/customers/customer/%s", "customer_id", "com.gojek.esb.fraud.EnrichedBookingLogMessage", "5000", "6000", "5000", outputMappings);

        EsExternalSourceConfig actualEsExternalSourceConfig = postProcessorConfig.getExternalSource().getEsConfig().get(0);
        assertEquals(expectedEsExternalSourceConfig.getHost(), actualEsExternalSourceConfig.getHost());
        assertEquals(expectedEsExternalSourceConfig.getQuery_param_pattern(), actualEsExternalSourceConfig.getQuery_param_pattern());
        assertEquals(expectedEsExternalSourceConfig.getQuery_param_variables(), actualEsExternalSourceConfig.getQuery_param_variables());
        assertEquals(expectedEsExternalSourceConfig.getType(), actualEsExternalSourceConfig.getType());
        assertEquals(expectedEsExternalSourceConfig.getRetry_timeout(), actualEsExternalSourceConfig.getRetry_timeout());
        assertEquals(expectedEsExternalSourceConfig.getSocket_timeout(), actualEsExternalSourceConfig.getSocket_timeout());
        assertEquals(expectedEsExternalSourceConfig.getStream_timeout(), actualEsExternalSourceConfig.getStream_timeout());
        assertEquals(expectedEsExternalSourceConfig.getOutputMapping().get("customer_profile"), actualEsExternalSourceConfig.getOutputMapping().get("customer_profile"));
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
    public void shouldThrowExceptionIfOnlyInvalidTypeIsPassed() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid config type");

        configuration = "{\"external_source\": {\"wrong_key\" : \"test\"}}";

        PostProcessorConfig.parse(configuration);
    }


    @Test
    public void shouldNotThrowExceptionIfTypesOtherThanHttpPassedWithHttp() {
        configuration = "{\"external_source\": {\n" +
            "    \"http\": [\n" +
                    "      {\n" +
                    "        \"endpoint\": \"http://localhost:8000\",\n" +
                    "        \"verb\": \"post\",\n" +
                    "        \"body_column_from_sql\": \"request_body\",\n" +
                    "        \"stream_timeout\": \"5000\",\n" +
                    "        \"connect_timeout\": \"5000\",\n" +
                    "        \"fail_on_errors\": \"true\", \n" +
                    "        \"headers\": {\n" +
                    "          \"content-type\": \"application/json\"\n" +
                    "        },\n" +
                    "        \"output_mapping\": {\n" +
                    "          \"surge_factor\": {\n" +
                    "            \"path\": \"$.data.tensor.values[0]\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      }\n" +
                    "    ],\n" +
                "\"wrong_key\" : \"test\"}}";

        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        HashMap<String, OutputMapping> outputMappings;
        OutputMapping outputMapping;
        HashMap<String, String> headerMap;
        headerMap = new HashMap<>();
        headerMap.put("content-type", "application/json");
        outputMappings = new HashMap<>();
        outputMapping = new OutputMapping("$.surge");
        outputMappings.put("surge_factor", outputMapping);

        HttpExternalSourceConfig expectedHttpExternalSourceConfig = new HttpExternalSourceConfig("http://localhost:8000",
                "post", "request_body", "5000", "5000", false, "", headerMap, outputMappings);

        HttpExternalSourceConfig actualHttpExternalSourceConfig = postProcessorConfig.getExternalSource().getHttpConfig().get(0);
        assertEquals(expectedHttpExternalSourceConfig.getBodyColumnFromSql(), actualHttpExternalSourceConfig.getBodyColumnFromSql());
        assertEquals(expectedHttpExternalSourceConfig.getEndpoint(), actualHttpExternalSourceConfig.getEndpoint());
        assertEquals(expectedHttpExternalSourceConfig.getConnectTimeout(), actualHttpExternalSourceConfig.getConnectTimeout());
        assertEquals(expectedHttpExternalSourceConfig.getStreamTimeout(), actualHttpExternalSourceConfig.getStreamTimeout());
        assertEquals(expectedHttpExternalSourceConfig.getVerb(), actualHttpExternalSourceConfig.getVerb());
    }

    @Test
    public void shouldReturnExternalSourceConfigMap() {
        configuration = "{\"external_source\": {\n" +
                "    \"http\": [\n" +
                "      {\n" +
                "        \"endpoint\": \"http://10.202.120.225/seldon/mlp-showcase/integrationtest/api/v0.1/predictions\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"es\": [\n" +
                "      {\n" +
                "        \"host\": \"10.240.60.227:9200\"\n" +
                "      }\n" +
                "    ]\n" +
                "  },\n" +
                "  \"transformers\": [\n" +
                "    {\n" +
                "      \"transformation_class\": \"com.gojek.daggers.postprocessor.FeatureTransformer\",\n" +
                "      \"transformation_arguments\": {\n" +
                "        \"value\": \"features\"\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        assertTrue(postProcessorConfig.getExternalSource().getHttpConfig().size() == 1);
        assertTrue(postProcessorConfig.getExternalSource().getEsConfig().size() == 1);
    }



    @Test
    public void shouldReturnPostProcessorConfig() {

        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null);
        assertEquals(externalSourceConfig, postProcessorConfig.getExternalSource());
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
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null);
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