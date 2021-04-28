package io.odpf.dagger.core.processors.postprocessors;

import com.jayway.jsonpath.InvalidJsonException;
import io.odpf.dagger.core.processors.PostProcessorConfig;
import io.odpf.dagger.core.processors.common.OutputMapping;
import io.odpf.dagger.core.processors.external.ExternalSourceConfig;
import io.odpf.dagger.core.processors.external.es.EsSourceConfig;
import io.odpf.dagger.core.processors.external.http.HttpSourceConfig;
import io.odpf.dagger.core.processors.external.pg.PgSourceConfig;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.transformers.TransformConfig;
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

    private final ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private PostProcessorConfig postProcessorConfig;
    private ArrayList<InternalSourceConfig> internalSource = new ArrayList<>();
    private List<TransformConfig> transformConfigs = new ArrayList<>();
    private String configuration = "{\n  \"external_source\": {\n    \"es\": [\n      {\n        \"host\": \"localhost:9200\",\n        \"output_mapping\": {\n          \"customer_profile\": {\n            \"path\": \"$._source\"\n          }\n        },\n        \"query_param_pattern\": \"/customers/customer/%s\",\n        \"query_param_variables\": \"customer_id\",\n        \"retry_timeout\": \"5000\",\n        \"socket_timeout\": \"6000\",\n        \"stream_timeout\": \"5000\",\n        \"type\": \"TestLogMessage\"\n      }\n    ],\n    \"http\": [\n      {\n        \"body_column_from_sql\": \"request_body\",\n        \"connect_timeout\": \"5000\",\n        \"endpoint\": \"http://localhost:8000\",\n        \"fail_on_errors\": \"true\",\n        \"headers\": {\n          \"content-type\": \"application/json\"\n        },\n        \"output_mapping\": {\n          \"surge_factor\": {\n            \"path\": \"$.data.tensor.values[0]\"\n          }\n        },\n        \"stream_timeout\": \"5000\",\n        \"verb\": \"post\"\n      }\n    ]\n  },\n  \"internal_source\":[\n  \t{\n    \"output_field\": \"event_timestamp\",\n    \"value\": \"CURRENT_TIMESTAMP\",\n    \"type\": \"function\"\n  \t},\n  \t{\n    \"output_field\": \"s2_id_level\",\n    \"value\": \"7\",\n    \"type\": \"constant\"\n\t }\n\t],\n  \"transformers\": [\n    {\n      \"transformation_arguments\": {\n        \"keyColumnName\": \"s2id\",\n        \"valueColumnName\": \"features\"\n      },\n      \"transformation_class\": \"test.postprocessor.FeatureTransformer\"\n    }\n  ]\n}";

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

    }

    @Test
    public void shouldReturnEsExternalSourceConfig() {
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        HashMap<String, OutputMapping> outputMappings;
        OutputMapping outputMapping;
        outputMappings = new HashMap<>();
        outputMapping = new OutputMapping("$._source");
        outputMappings.put("customer_profile", outputMapping);
    }

    @Test
    public void shouldReturnTransformConfig() {
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        HashMap<String, Object> transformationArguments;
        transformationArguments = new HashMap<>();
        transformationArguments.put("keyColumnName", "s2id");
        transformationArguments.put("valueColumnName", "features");
        String transformationClass = "test.postprocessor.FeatureTransformer";
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
    public void shouldBeEmptyWhenNoneOfTheConfigsExist() {
        postProcessorConfig = new PostProcessorConfig(null, null, null);

        assertTrue(postProcessorConfig.isEmpty());
    }


    @Test
    public void shouldNotBeEmptyWhenExternalSourceHasHttpConfigExist() {
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        http.add(new HttpSourceConfig("", "", "", "", "", "", false, "", "", new HashMap<>(), new HashMap<>(), "metricId_01", false));
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg, new ArrayList<>());
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, null);

        assertFalse(postProcessorConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenExternalSourceHasEsConfigExist() {
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        es.add(new EsSourceConfig("", "", "", "", "", "", "", "", "", "", "", "", false, new HashMap<>(), "metricId_01", false));
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg, new ArrayList<>());
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, null);

        assertFalse(postProcessorConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenExternalSourceHasPgConfigExist() {
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        pg.add(new PgSourceConfig("", "", "", "", "", "", "", "", new HashMap<>(), "", "", "", "", true, "metricId_01", false));
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg, new ArrayList<>());
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, null);

        assertFalse(postProcessorConfig.isEmpty());
    }


    @Test
    public void shouldBeEmptyWhenExternalSourceHasEmptyConfig() {
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg, new ArrayList<>());
        postProcessorConfig = new PostProcessorConfig(externalSourceConfig, null, null);

        assertTrue(postProcessorConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenInternalSourceExist() {
        ArrayList<InternalSourceConfig> internalSourceConfigs = new ArrayList<>();
        internalSourceConfigs.add(new InternalSourceConfig("outputField", "value", "type"));
        postProcessorConfig = new PostProcessorConfig(null, null, internalSourceConfigs);

        assertFalse(postProcessorConfig.isEmpty());
    }

    @Test
    public void shouldNotBeEmptyWhenTransformConfigsExist() {
        transformConfigs.add(new TransformConfig("testClass", new HashMap<>()));
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
        Map<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyValue", "key");
        transformConfigs.add(new TransformConfig("test.postprocessor.XTransformer", transformationArguments));
        postProcessorConfig = new PostProcessorConfig(null, transformConfigs, internalSource);
        assertEquals(transformConfigs, postProcessorConfig.getTransformers());
    }

    @Test
    public void shouldBeTrueWhenExternalSourceExists() {
        ArrayList<HttpSourceConfig> http = new ArrayList<>();
        ArrayList<PgSourceConfig> pg = new ArrayList<>();
        ArrayList<EsSourceConfig> es = new ArrayList<>();
        es.add(new EsSourceConfig("", "", "", "", "", "", "", "", "", "", "", "", false, new HashMap<>(), "metricId_01", false));
        ExternalSourceConfig externalSourceConfig = new ExternalSourceConfig(http, es, pg, new ArrayList<>());
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
        internalSource.add(new InternalSourceConfig("outputField", "value", "type"));
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
        Map<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyValue", "key");
        transformConfigs.add(new TransformConfig("test.postprocessor.XTransformer", transformationArguments));
        postProcessorConfig = new PostProcessorConfig(null, transformConfigs, internalSource);
        assertTrue(postProcessorConfig.hasTransformConfigs());
    }

    @Test
    public void shouldBeFalseWhenTransformerSourceDoesNotExists() {
        postProcessorConfig = new PostProcessorConfig(null, null, internalSource);
        assertFalse(postProcessorConfig.hasTransformConfigs());
    }

    @Test
    public void shouldReturnTrueForHasSQLTransformerIfTransformConfigContainsSqlTransformer() {
        String configuration = "{ \"external_source\": { \"es\": [ { \"host\": \"localhost:9200\", \"output_mapping\": { \"customer_profile\": { \"path\": \"$._source\" } }, \"query_param_pattern\": \"/customers/customer/%s\", \"query_param_variables\": \"customer_id\", \"retry_timeout\": \"5000\", \"socket_timeout\": \"6000\", \"stream_timeout\": \"5000\", \"type\": \"TestLogMessage\" } ], \"http\": [ { \"body_column_from_sql\": \"request_body\", \"connect_timeout\": \"5000\", \"endpoint\": \"http://localhost:8000\", \"fail_on_errors\": \"true\", \"headers\": { \"content-type\": \"application/json\" }, \"output_mapping\": { \"surge_factor\": { \"path\": \"$.data.tensor.values[0]\" } }, \"stream_timeout\": \"5000\", \"verb\": \"post\" } ] }, \"internal_source\":[ { \"output_field\": \"event_timestamp\", \"value\": \"CURRENT_TIMESTAMP\", \"type\": \"function\" }, { \"output_field\": \"s2_id_level\", \"value\": \"7\", \"type\": \"constant\" } ], \"transformers\": [ { \"transformation_arguments\": { \"sqlQuery\": \"SELECT * from data_stream\" }, \"transformation_class\": \"com.gojek.dagger.transformer.SQLTransformer\" } ] }";
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        assertTrue(postProcessorConfig.hasSQLTransformer());
    }

    @Test
    public void shouldNotReturnTrueForHasSQLTransformerIfTransformConfigDoesNotContainSqlTransformer() {
        String configuration = "{ \"external_source\": { \"es\": [ { \"host\": \"localhost:9200\", \"output_mapping\": { \"customer_profile\": { \"path\": \"$._source\" } }, \"query_param_pattern\": \"/customers/customer/%s\", \"query_param_variables\": \"customer_id\", \"retry_timeout\": \"5000\", \"socket_timeout\": \"6000\", \"stream_timeout\": \"5000\", \"type\": \"TestLogMessage\" } ], \"http\": [ { \"body_column_from_sql\": \"request_body\", \"connect_timeout\": \"5000\", \"endpoint\": \"http://localhost:8000\", \"fail_on_errors\": \"true\", \"headers\": { \"content-type\": \"application/json\" }, \"output_mapping\": { \"surge_factor\": { \"path\": \"$.data.tensor.values[0]\" } }, \"stream_timeout\": \"5000\", \"verb\": \"post\" } ] }, \"internal_source\":[ { \"output_field\": \"event_timestamp\", \"value\": \"CURRENT_TIMESTAMP\", \"type\": \"function\" }, { \"output_field\": \"s2_id_level\", \"value\": \"7\", \"type\": \"constant\" } ], \"transformers\": [ { \"transformation_arguments\": { \"sqlQuery\": \"SELECT * from data_stream\" }, \"transformation_class\": \"com.gojek.dagger.transformer.DeDuplicationTransformer\" } ] }";
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        assertFalse(postProcessorConfig.hasSQLTransformer());
    }

    @Test
    public void shouldNotReturnTrueForHasSQLTransformerIfTransformConfigDoesNotExist() {
        String configuration = "{ \"external_source\": { \"es\": [ { \"host\": \"localhost:9200\", \"output_mapping\": { \"customer_profile\": { \"path\": \"$._source\" } }, \"query_param_pattern\": \"/customers/customer/%s\", \"query_param_variables\": \"customer_id\", \"retry_timeout\": \"5000\", \"socket_timeout\": \"6000\", \"stream_timeout\": \"5000\", \"type\": \"TestLogMessage\" } ], \"http\": [ { \"body_column_from_sql\": \"request_body\", \"connect_timeout\": \"5000\", \"endpoint\": \"http://localhost:8000\", \"fail_on_errors\": \"true\", \"headers\": { \"content-type\": \"application/json\" }, \"output_mapping\": { \"surge_factor\": { \"path\": \"$.data.tensor.values[0]\" } }, \"stream_timeout\": \"5000\", \"verb\": \"post\" } ] }, \"internal_source\":[ { \"output_field\": \"event_timestamp\", \"value\": \"CURRENT_TIMESTAMP\", \"type\": \"function\" }, { \"output_field\": \"s2_id_level\", \"value\": \"7\", \"type\": \"constant\" } ] }";
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        assertFalse(postProcessorConfig.hasSQLTransformer());
    }

    @Test
    public void shouldReturnTrueForHasSQLTransformerIfAnyOneTransformConfigContainsSQLTransformer() {
        String configuration = "{ \"external_source\": { \"es\": [ { \"host\": \"localhost:9200\", \"output_mapping\": { \"customer_profile\": { \"path\": \"$._source\" } }, \"query_param_pattern\": \"/customers/customer/%s\", \"query_param_variables\": \"customer_id\", \"retry_timeout\": \"5000\", \"socket_timeout\": \"6000\", \"stream_timeout\": \"5000\", \"type\": \"TestLogMessage\" } ], \"http\": [ { \"body_column_from_sql\": \"request_body\", \"connect_timeout\": \"5000\", \"endpoint\": \"http://localhost:8000\", \"fail_on_errors\": \"true\", \"headers\": { \"content-type\": \"application/json\" }, \"output_mapping\": { \"surge_factor\": { \"path\": \"$.data.tensor.values[0]\" } }, \"stream_timeout\": \"5000\", \"verb\": \"post\" } ] }, \"internal_source\":[ { \"output_field\": \"event_timestamp\", \"value\": \"CURRENT_TIMESTAMP\", \"type\": \"function\" }, { \"output_field\": \"s2_id_level\", \"value\": \"7\", \"type\": \"constant\" } ], \"transformers\": [ { \"transformation_arguments\": { \"sqlQuery\": \"SELECT * from data_stream\" }, \"transformation_class\": \"com.gojek.dagger.transformer.SQLTransformer\" }, { \"transformation_arguments\": { \"arg1\": \"test\" }, \"transformation_class\": \"com.gojek.dagger.transformer.Test\" } ] }";
        PostProcessorConfig postProcessorConfig = PostProcessorConfig.parse(configuration);
        assertTrue(postProcessorConfig.hasSQLTransformer());
    }

}