package com.gojek.daggers.postprocessor.parser;

import com.jayway.jsonpath.InvalidJsonException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.*;

public class ExternalSourceConfigTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private String configuration = "{\n" +
            "  \"http\": [\n" +
            "    {\n" +
            "      \"endpoint\": \"http://localhost:8000\",\n" +
            "      \"verb\": \"post\",\n" +
            "      \"body_field\": \"request_body\",\n" +
            "      \"stream_timeout\": \"5000\",\n" +
            "      \"connect_timeout\": \"5000\",\n" +
            "      \"headers\": {\n" +
            "        \"content-type\": \"application/json\"\n" +
            "      },\n" +
            "      \"output_mapping\": {\n" +
            "        \"surge_factor\": {\n" +
            "          \"path\": \"$.surge\"\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}\n";

    @Test
    public void shouldParseGivenConfiguration() {
        ExternalSourceConfig externalSourceConfig = ExternalSourceConfig.parse(configuration);
        assertNotNull(externalSourceConfig);
    }

    @Test
    public void shouldReturnKeys() {
        ExternalSourceConfig externalSourceConfig = ExternalSourceConfig.parse(configuration);
        HashSet<String> expectedKeys = new HashSet<>();
        expectedKeys.add("http");
        assertArrayEquals(expectedKeys.toArray(), externalSourceConfig.getExternalSourceKeys().toArray());
    }

    @Test
    public void shouldReturnColumns() {
        ExternalSourceConfig externalSourceConfig = ExternalSourceConfig.parse(configuration);
        ArrayList<String> expectedColumnNames = new ArrayList<>();
        expectedColumnNames.add("surge_factor");
        assertArrayEquals(expectedColumnNames.toArray(), externalSourceConfig.getColumns().toArray());
    }

    @Test
    public void shouldReturnHttpExternalSourceConfig() {
        ExternalSourceConfig externalSourceConfig = ExternalSourceConfig.parse(configuration);
        HashMap<String, OutputMapping> outputMappings;
        OutputMapping outputMapping;
        HashMap<String, String> headerMap;
        headerMap = new HashMap<>();
        headerMap.put("content-type", "application/json");
        outputMappings = new HashMap<>();
        outputMapping = new OutputMapping("$.surge");
        outputMappings.put("surge_factor", outputMapping);

        HttpExternalSourceConfig expectedHttpExternalSourceConfig = new HttpExternalSourceConfig("http://localhost:8000",
                "post", "request_body", "5000", "5000", false, headerMap, outputMappings);

        HttpExternalSourceConfig actualHttpExternalSourceConfig = externalSourceConfig.getHttpExternalSourceConfig().get(0);
        assertEquals(expectedHttpExternalSourceConfig.getBodyField(), actualHttpExternalSourceConfig.getBodyField());
        assertEquals(expectedHttpExternalSourceConfig.getEndpoint(), actualHttpExternalSourceConfig.getEndpoint());
        assertEquals(expectedHttpExternalSourceConfig.getConnectTimeout(), actualHttpExternalSourceConfig.getConnectTimeout());
        assertEquals(expectedHttpExternalSourceConfig.getStreamTimeout(), actualHttpExternalSourceConfig.getStreamTimeout());
        assertEquals(expectedHttpExternalSourceConfig.getVerb(), actualHttpExternalSourceConfig.getVerb());
    }

    @Test
    public void shouldThrowExceptionIfInvalidJsonConfigurationPassed() {
        expectedException.expect(InvalidJsonException.class);
        expectedException.expectMessage("Invalid JSON Given for EXTERNAL_SOURCE");
        configuration = "test";
        ExternalSourceConfig.parse(configuration);
    }

    @Test
    public void shouldThrowExceptionIfTypesOtherThanHttpPassed() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid config type");

        configuration = "{\n" +
                "  \"es\": [\n" +
                "    {\n" +
                "      \"endpoint\": \"http://localhost:8000\"\n" +
                "    }\n" +
                "  ]\n" +
                "}\n";

        ExternalSourceConfig.parse(configuration);
    }

}