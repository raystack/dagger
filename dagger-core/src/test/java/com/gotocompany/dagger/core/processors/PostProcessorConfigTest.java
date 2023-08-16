package com.gotocompany.dagger.core.processors;

import com.gotocompany.dagger.core.processors.common.OutputMapping;
import com.jayway.jsonpath.InvalidJsonException;
import com.gotocompany.dagger.core.processors.external.ExternalSourceConfig;
import com.gotocompany.dagger.core.processors.external.es.EsSourceConfig;
import com.gotocompany.dagger.core.processors.external.http.HttpSourceConfig;
import com.gotocompany.dagger.core.processors.external.pg.PgSourceConfig;
import com.gotocompany.dagger.core.processors.internal.InternalSourceConfig;
import com.gotocompany.dagger.core.processors.transformers.TransformConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PostProcessorConfigTest {

    private final ExternalSourceConfig defaultExternalSourceConfig = new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private PostProcessorConfig defaultPostProcessorConfig;
    private ArrayList<InternalSourceConfig> defaultInternalSource = new ArrayList<>();
    private List<TransformConfig> transformConfigs = new ArrayList<>();
    private String defaultConfiguration = "{\n  \"external_source\": {\n    \"es\": [\n      {\n        \"host\": \"localhost\",\n \"port\": \"9200\",\n        \"output_mapping\": {\n          \"customer_profile\": {\n            \"path\": \"$._source\"\n          }\n        },\n        \"endpoint_pattern\": \"/customers/customer/%s\",\n        \"endpoint_variables\": \"customer_id\",\n        \"retry_timeout\": \"5000\",\n        \"socket_timeout\": \"6000\",\n        \"stream_timeout\": \"5000\",\n        \"type\": \"TestLogMessage\"\n      }\n    ],\n    \"http\": [\n      {\n        \"body_column_from_sql\": \"request_body\",\n        \"connect_timeout\": \"5000\",\n        \"endpoint\": \"http://localhost:8000\",\n        \"fail_on_errors\": \"true\",\n        \"headers\": {\n          \"content-type\": \"application/json\"\n        },\n        \"output_mapping\": {\n          \"surge_factor\": {\n            \"path\": \"$.data.tensor.values[0]\"\n          }\n        },\n        \"stream_timeout\": \"5000\",\n        \"verb\": \"post\"\n      }\n    ]\n  },\n  \"internal_source\":[\n  \t{\n    \"output_field\": \"event_timestamp\",\n    \"value\": \"CURRENT_TIMESTAMP\",\n    \"type\": \"function\"\n  \t},\n  \t{\n    \"output_field\": \"s2_id_level\",\n    \"value\": \"7\",\n    \"type\": \"constant\"\n\t }\n\t],\n  \"transformers\": [\n    {\n      \"transformation_arguments\": {\n        \"keyColumnName\": \"s2id\",\n        \"valueColumnName\": \"features\"\n      },\n      \"transformation_class\": \"test.postprocessor.FeatureTransformer\"\n    }\n  ]\n}";

    @Test
    public void shouldParseGivenConfiguration() {
        defaultPostProcessorConfig = PostProcessorConfig.parse(defaultConfiguration);

        assertNotNull(defaultPostProcessorConfig);
    }

    @Test
    public void shouldReturnColumns() {
        defaultPostProcessorConfig = PostProcessorConfig.parse(defaultConfiguration);
        ArrayList<String> expectedColumnNames = new ArrayList<>();
        expectedColumnNames.add("surge_factor");
        expectedColumnNames.add("customer_profile");
        expectedColumnNames.add("event_timestamp");
        expectedColumnNames.add("s2_id_level");

        assertArrayEquals(expectedColumnNames.toArray(), defaultPostProcessorConfig.getOutputColumnNames().toArray());
    }

    @Test
    public void shouldReturnHttpExternalSourceConfig() {
        defaultPostProcessorConfig = PostProcessorConfig.parse(defaultConfiguration);
        HashMap<String, OutputMapping> outputMappings;
        OutputMapping outputMapping;
        HashMap<String, String> headerMap;
        headerMap = new HashMap<>();
        headerMap.put("content-type", "application/json");
        outputMappings = new HashMap<>();
        outputMapping = new OutputMapping("$.data.tensor.values[0]");
        outputMappings.put("surge_factor", outputMapping);

        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("http://localhost:8000", "", "post", null, null, null, null, "5000", "5000", true, null, null, null, headerMap, outputMappings, null, false);

        assertEquals(httpSourceConfig, defaultPostProcessorConfig.getExternalSource().getHttpConfig().get(0));
    }

    @Test
    public void shouldReturnEsExternalSourceConfig() {
        defaultPostProcessorConfig = PostProcessorConfig.parse(defaultConfiguration);
        HashMap<String, OutputMapping> outputMappings;
        OutputMapping outputMapping;
        outputMappings = new HashMap<>();
        outputMapping = new OutputMapping("$._source");
        outputMappings.put("customer_profile", outputMapping);

        EsSourceConfig esSourceConfig = new EsSourceConfig("localhost", "9200", null, null, "/customers/customer/%s", "customer_id", "TestLogMessage", null, null, "5000", "6000", "5000", false, outputMappings, null, false);

        assertEquals(esSourceConfig, defaultPostProcessorConfig.getExternalSource().getEsConfig().get(0));
    }

    @Test
    public void shouldReturnTransformConfig() {
        defaultPostProcessorConfig = PostProcessorConfig.parse(defaultConfiguration);
        HashMap<String, Object> transformationArguments;
        transformationArguments = new HashMap<>();
        transformationArguments.put("keyColumnName", "s2id");
        transformationArguments.put("valueColumnName", "features");
        String transformationClass = "test.postprocessor.FeatureTransformer";
        TransformConfig expectedTransformerConfig = new TransformConfig(transformationClass, transformationArguments);

        TransformConfig actualTransformerConfig = defaultPostProcessorConfig.getTransformers().get(0);

        assertEquals(expectedTransformerConfig.getTransformationArguments(), actualTransformerConfig.getTransformationArguments());
        assertEquals(expectedTransformerConfig.getTransformationClass(), actualTransformerConfig.getTransformationClass());
    }

    @Test
    public void shouldThrowExceptionIfInvalidJsonConfigurationPassed() {
        expectedException.expect(InvalidJsonException.class);
        expectedException.expectMessage("Invalid JSON Given for PROCESSOR_POSTPROCESSOR_CONFIG");

        defaultConfiguration = "test";

        PostProcessorConfig.parse(defaultConfiguration);
    }

    @Test
    public void shouldBeEmptyWhenNoneOfTheConfigsExist() {
        defaultPostProcessorConfig = new PostProcessorConfig(null, null, null);

        assertTrue(defaultPostProcessorConfig.isEmpty());
    }


    @Test
    public void shouldNotBeEmptyWhenExternalSourceHasHttpConfigExist() {
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        http.add(new HttpSourceConfig("", "", "", "", "", "", "", "", "", false, null, "", "", new HashMap<>(), new HashMap<>(), "metricId_01", false));
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg, new ArrayList<>());
        defaultPostProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, null);

        assertFalse(defaultPostProcessorConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenExternalSourceHasEsConfigExist() {
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        es.add(new EsSourceConfig("", "", "", "", "", "", "", "", "", "", "", "", false, new HashMap<>(), "metricId_01", false));
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg, new ArrayList<>());
        defaultPostProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, null);

        assertFalse(defaultPostProcessorConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenExternalSourceHasPgConfigExist() {
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        pg.add(new PgSourceConfig("", "", "", "", "", "", "", "", new HashMap<>(), "", "", "", "", true, "metricId_01", false));
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg, new ArrayList<>());
        defaultPostProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, null);

        assertFalse(defaultPostProcessorConfig.isEmpty());
    }


    @Test
    public void shouldBeEmptyWhenExternalSourceHasEmptyConfig() {
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg, new ArrayList<>());
        defaultPostProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, null);

        assertTrue(defaultPostProcessorConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenInternalSourceExist() {
        ArrayList<InternalSourceConfig> internalSourceConfigs = new ArrayList<>();
        internalSourceConfigs.add(new InternalSourceConfig("outputField", "value", "type", null));
        defaultPostProcessorConfig = new PostProcessorConfig(null, null, internalSourceConfigs);

        assertFalse(defaultPostProcessorConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenTransformConfigsExist() {
        transformConfigs.add(new TransformConfig("testClass", new HashMap<>()));
        defaultPostProcessorConfig = new PostProcessorConfig(null, transformConfigs, null);

        assertFalse(defaultPostProcessorConfig.isEmpty());
    }

    @Test
    public void shouldReturnExternalSourceConfig() {
        defaultPostProcessorConfig = new PostProcessorConfig(defaultExternalSourceConfig, null, defaultInternalSource);

        assertEquals(defaultExternalSourceConfig, defaultPostProcessorConfig.getExternalSource());
    }

    @Test
    public void shouldReturnInternalSourceConfig() {
        defaultPostProcessorConfig = new PostProcessorConfig(defaultExternalSourceConfig, null, defaultInternalSource);
        assertEquals(defaultInternalSource, defaultPostProcessorConfig.getInternalSource());
    }

    @Test
    public void shouldReturnTransformConfigs() {
        Map<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyValue", "key");
        transformConfigs.add(new TransformConfig("test.postprocessor.XTransformer", transformationArguments));
        defaultPostProcessorConfig = new PostProcessorConfig(null, transformConfigs, defaultInternalSource);
        assertEquals(transformConfigs, defaultPostProcessorConfig.getTransformers());
    }

    @Test
    public void shouldBeTrueWhenExternalSourceExists() {
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        es.add(new EsSourceConfig("", "", "", "", "", "", "", "", "", "", "", "", false, new HashMap<>(), "metricId_01", false));
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg, new ArrayList<>());
        defaultPostProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, defaultInternalSource);
        assertTrue(defaultPostProcessorConfig.hasExternalSource());
    }

    @Test
    public void shouldBeFalseWhenExternalSourceDoesNotExists() {
        defaultPostProcessorConfig = new PostProcessorConfig(null, null, defaultInternalSource);
        assertFalse(defaultPostProcessorConfig.hasExternalSource());
    }

    @Test
    public void shouldNotHaveInternalSourceWhenInternalSourceIsEmpty() {
        defaultPostProcessorConfig = new PostProcessorConfig(defaultExternalSourceConfig, null, defaultInternalSource);
        assertFalse(defaultPostProcessorConfig.hasInternalSource());
    }

    @Test
    public void shouldHaveInternalSourceWhenInternalSourceIsNotEmpty() {
        ArrayList<InternalSourceConfig> internalSource = new ArrayList<>();
        internalSource.add(new InternalSourceConfig("outputField", "value", "type", null));
        defaultPostProcessorConfig = new PostProcessorConfig(defaultExternalSourceConfig, null, this.defaultInternalSource);
        assertFalse(defaultPostProcessorConfig.hasInternalSource());
    }

    @Test
    public void shouldBeFalseWhenInternalSourceDoesNotExists() {
        defaultPostProcessorConfig = new PostProcessorConfig(defaultExternalSourceConfig, null, null);
        assertFalse(defaultPostProcessorConfig.hasInternalSource());
    }

    @Test
    public void shouldBeTrueWhenTransformerSourceExists() {
        Map<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyValue", "key");
        transformConfigs.add(new TransformConfig("test.postprocessor.XTransformer", transformationArguments));
        defaultPostProcessorConfig = new PostProcessorConfig(null, transformConfigs, defaultInternalSource);
        assertTrue(defaultPostProcessorConfig.hasTransformConfigs());
    }

    @Test
    public void shouldBeFalseWhenTransformerSourceDoesNotExists() {
        defaultPostProcessorConfig = new PostProcessorConfig(null, null, defaultInternalSource);
        assertFalse(defaultPostProcessorConfig.hasTransformConfigs());
    }

    @Test
    public void shouldReturnTrueForHasSQLTransformerIfTransformConfigContainsSqlTransformer() {
        String configuration = "{ \"external_source\": { \"es\": [ { \"host\": \"localhost:9200\", \"output_mapping\": { \"customer_profile\": { \"path\": \"$._source\" } }, \"query_param_pattern\": \"/customers/customer/%s\", \"query_param_variables\": \"customer_id\", \"retry_timeout\": \"5000\", \"socket_timeout\": \"6000\", \"stream_timeout\": \"5000\", \"type\": \"TestLogMessage\" } ], \"http\": [ { \"body_column_from_sql\": \"request_body\", \"connect_timeout\": \"5000\", \"endpoint\": \"http://localhost:8000\", \"fail_on_errors\": \"true\", \"headers\": { \"content-type\": \"application/json\" }, \"output_mapping\": { \"surge_factor\": { \"path\": \"$.data.tensor.values[0]\" } }, \"stream_timeout\": \"5000\", \"verb\": \"post\" } ] }, \"internal_source\":[ { \"output_field\": \"event_timestamp\", \"value\": \"CURRENT_TIMESTAMP\", \"type\": \"function\" }, { \"output_field\": \"s2_id_level\", \"value\": \"7\", \"type\": \"constant\" } ], \"transformers\": [ { \"transformation_arguments\": { \"sqlQuery\": \"SELECT * from data_stream\" }, \"transformation_class\": \"SQLTransformer\" } ] }";
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        assertTrue(postProcessorConfig.hasSQLTransformer());
    }

    @Test
    public void shouldNotReturnTrueForHasSQLTransformerIfTransformConfigDoesNotContainSqlTransformer() {
        String configuration = "{ \"external_source\": { \"es\": [ { \"host\": \"localhost:9200\", \"output_mapping\": { \"customer_profile\": { \"path\": \"$._source\" } }, \"query_param_pattern\": \"/customers/customer/%s\", \"query_param_variables\": \"customer_id\", \"retry_timeout\": \"5000\", \"socket_timeout\": \"6000\", \"stream_timeout\": \"5000\", \"type\": \"TestLogMessage\" } ], \"http\": [ { \"body_column_from_sql\": \"request_body\", \"connect_timeout\": \"5000\", \"endpoint\": \"http://localhost:8000\", \"fail_on_errors\": \"true\", \"headers\": { \"content-type\": \"application/json\" }, \"output_mapping\": { \"surge_factor\": { \"path\": \"$.data.tensor.values[0]\" } }, \"stream_timeout\": \"5000\", \"verb\": \"post\" } ] }, \"internal_source\":[ { \"output_field\": \"event_timestamp\", \"value\": \"CURRENT_TIMESTAMP\", \"type\": \"function\" }, { \"output_field\": \"s2_id_level\", \"value\": \"7\", \"type\": \"constant\" } ], \"transformers\": [ { \"transformation_arguments\": { \"sqlQuery\": \"SELECT * from data_stream\" }, \"transformation_class\": \"com.gotocompany.dagger.transformer.DeDuplicationTransformer\" } ] }";
        defaultPostProcessorConfig = PostProcessorConfig.parse(configuration);
        assertFalse(defaultPostProcessorConfig.hasSQLTransformer());
    }

    @Test
    public void shouldNotReturnTrueForHasSQLTransformerIfTransformConfigDoesNotExist() {
        String configuration = "{ \"external_source\": { \"es\": [ { \"host\": \"localhost:9200\", \"output_mapping\": { \"customer_profile\": { \"path\": \"$._source\" } }, \"query_param_pattern\": \"/customers/customer/%s\", \"query_param_variables\": \"customer_id\", \"retry_timeout\": \"5000\", \"socket_timeout\": \"6000\", \"stream_timeout\": \"5000\", \"type\": \"TestLogMessage\" } ], \"http\": [ { \"body_column_from_sql\": \"request_body\", \"connect_timeout\": \"5000\", \"endpoint\": \"http://localhost:8000\", \"fail_on_errors\": \"true\", \"headers\": { \"content-type\": \"application/json\" }, \"output_mapping\": { \"surge_factor\": { \"path\": \"$.data.tensor.values[0]\" } }, \"stream_timeout\": \"5000\", \"verb\": \"post\" } ] }, \"internal_source\":[ { \"output_field\": \"event_timestamp\", \"value\": \"CURRENT_TIMESTAMP\", \"type\": \"function\" }, { \"output_field\": \"s2_id_level\", \"value\": \"7\", \"type\": \"constant\" } ] }";
        defaultPostProcessorConfig = PostProcessorConfig.parse(configuration);
        assertFalse(defaultPostProcessorConfig.hasSQLTransformer());
    }

    @Test
    public void shouldReturnTrueForHasSQLTransformerIfAnyOneTransformConfigContainsSQLTransformer() {
        String configuration = "{ \"external_source\": { \"es\": [ { \"host\": \"localhost:9200\", \"output_mapping\": { \"customer_profile\": { \"path\": \"$._source\" } }, \"query_param_pattern\": \"/customers/customer/%s\", \"query_param_variables\": \"customer_id\", \"retry_timeout\": \"5000\", \"socket_timeout\": \"6000\", \"stream_timeout\": \"5000\", \"type\": \"TestLogMessage\" } ], \"http\": [ { \"body_column_from_sql\": \"request_body\", \"connect_timeout\": \"5000\", \"endpoint\": \"http://localhost:8000\", \"fail_on_errors\": \"true\", \"headers\": { \"content-type\": \"application/json\" }, \"output_mapping\": { \"surge_factor\": { \"path\": \"$.data.tensor.values[0]\" } }, \"stream_timeout\": \"5000\", \"verb\": \"post\" } ] }, \"internal_source\":[ { \"output_field\": \"event_timestamp\", \"value\": \"CURRENT_TIMESTAMP\", \"type\": \"function\" }, { \"output_field\": \"s2_id_level\", \"value\": \"7\", \"type\": \"constant\" } ], \"transformers\": [ { \"transformation_arguments\": { \"sqlQuery\": \"SELECT * from data_stream\" }, \"transformation_class\": \"SQLTransformer\" }, { \"transformation_arguments\": { \"arg1\": \"test\" }, \"transformation_class\": \"com.gotocompany.dagger.transformer.Test\" } ] }";
        defaultPostProcessorConfig = PostProcessorConfig.parse(configuration);
        assertTrue(defaultPostProcessorConfig.hasSQLTransformer());
    }

    @Test
    public void shouldParseInternalProcessorConfigForInternalSourceConfig() {
        String configuration = "{\"internal_source\":[{\"output_field\":\"payload\",\"value\":\"JSON_PAYLOAD\",\"type\":\"function\",\"internal_processor_config\":{\"schema_proto_class\":\"com.foo.bar.RestaurantMessage\"}}]}";
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        assertNotNull(postProcessorConfig.getInternalSource().get(0).getInternalProcessorConfig());
    }

    @Test
    public void shouldParseEndpointVariablesConfig() {
        String configuration = "{\"external_source\":{\"es\":[{\"host\":\"localhost\",\"port\":\"9200\",\"output_mapping\":{\"customer_profile\":{\"path\":\"$._source\"}},\"endpoint_pattern\":\"/customers/customer/%s\",\"endpoint_variables\":\"customer_id\",\"retry_timeout\":\"5000\",\"socket_timeout\":\"6000\",\"stream_timeout\":\"5000\",\"type\":\"TestLogMessage\"}],\"http\":[{\"body_column_from_sql\":\"request_body\",\"connect_timeout\":\"5000\",\"endpoint\":\"http://localhost:8000/%s\",\"endpoint_variables\":\"some-id\",\"fail_on_errors\":\"true\",\"headers\":{\"content-type\":\"application/json\"},\"output_mapping\":{\"surge_factor\":{\"path\":\"$.data.tensor.values[0]\"}},\"stream_timeout\":\"5000\",\"verb\":\"put\"}]},\"internal_source\":[{\"output_field\":\"event_timestamp\",\"value\":\"CURRENT_TIMESTAMP\",\"type\":\"function\"},{\"output_field\":\"s2_id_level\",\"value\":\"7\",\"type\":\"constant\"}],\"transformers\":[{\"transformation_arguments\":{\"keyColumnName\":\"s2id\",\"valueColumnName\":\"features\"},\"transformation_class\":\"test.postprocessor.FeatureTransformer\"}]}";
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        assertEquals("some-id", postProcessorConfig.getExternalSource().getHttpConfig().get(0).getEndpointVariables());
    }

    @Test
    public void shouldParseEmptyEndpointVariablesConfig() {
        String configuration = "{\"external_source\":{\"es\":[{\"host\":\"localhost\",\"port\":\"9200\",\"output_mapping\":{\"customer_profile\":{\"path\":\"$._source\"}},\"endpoint_pattern\":\"/customers/customer/%s\",\"endpoint_variables\":\"customer_id\",\"retry_timeout\":\"5000\",\"socket_timeout\":\"6000\",\"stream_timeout\":\"5000\",\"type\":\"TestLogMessage\"}],\"http\":[{\"body_column_from_sql\":\"request_body\",\"connect_timeout\":\"5000\",\"endpoint\":\"http://localhost:8000/%s\",\"fail_on_errors\":\"true\",\"headers\":{\"content-type\":\"application/json\"},\"output_mapping\":{\"surge_factor\":{\"path\":\"$.data.tensor.values[0]\"}},\"stream_timeout\":\"5000\",\"verb\":\"put\"}]},\"internal_source\":[{\"output_field\":\"event_timestamp\",\"value\":\"CURRENT_TIMESTAMP\",\"type\":\"function\"},{\"output_field\":\"s2_id_level\",\"value\":\"7\",\"type\":\"constant\"}],\"transformers\":[{\"transformation_arguments\":{\"keyColumnName\":\"s2id\",\"valueColumnName\":\"features\"},\"transformation_class\":\"test.postprocessor.FeatureTransformer\"}]}";
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        assertEquals(null, postProcessorConfig.getExternalSource().getHttpConfig().get(0).getEndpointVariables());
    }
}
