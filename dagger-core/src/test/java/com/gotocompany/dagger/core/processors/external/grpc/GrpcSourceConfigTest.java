package com.gotocompany.dagger.core.processors.external.grpc;


import com.google.gson.annotations.SerializedName;
import com.gotocompany.dagger.core.processors.common.OutputMapping;
import com.gotocompany.dagger.core.processors.external.http.HttpSourceConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class GrpcSourceConfigTest {

    private HashMap<String, String> headerMap;
    private GrpcSourceConfig grpcSourceConfig;
    private String endpoint;
    private int servicePort;
    private String requestPattern;
    private String requestVariables;
    private String streamTimeout;
    private String connectTimeout;
    private boolean failOnErrors;
    private String defaultGrpcStencilUrl;
    private String type;
    @SerializedName(value = "headers", alternate = {"Headers", "HEADERS"})
    private Map<String, OutputMapping> outputMappings;
    private OutputMapping outputMapping;
    @SerializedName(value = "metricId", alternate = {"MetricId", "METRICID"})
    private String metricId;
    private String grpcRequestProtoSchema;
    private String grpcResponseProtoSchema;
    private String grpcMethodUrl;
    private boolean retainResponseType;
    private int capacity;

    @Before
    public void setup() {
        headerMap = new HashMap<>();
        headerMap.put("content-type", "application/json");
        outputMappings = new HashMap<>();
        outputMapping = new OutputMapping("data.value");
        outputMappings.put("surge_factor", outputMapping);
        streamTimeout = "123";
        endpoint = "localhost";
        servicePort = 5000;
        requestPattern = "/customers/customer/%s";
        requestVariables = "customer_id";
        connectTimeout = "234";
        failOnErrors = false;
        defaultGrpcStencilUrl = "http://localhost/feast-proto/latest";
        type = "InputProtoMessage";
        capacity = 345;
        metricId = "metricId-http-01";
        retainResponseType = false;
        grpcMethodUrl = "test/TestMethod";
        grpcResponseProtoSchema = "ResponseMessage";
        grpcRequestProtoSchema = "RequestMessage";
        grpcSourceConfig = new GrpcSourceConfigBuilder().setEndpoint(endpoint).setServicePort(servicePort).setGrpcRequestProtoSchema(grpcRequestProtoSchema).setGrpcResponseProtoSchema(grpcResponseProtoSchema).setGrpcMethodUrl(grpcMethodUrl).setRequestPattern(requestPattern).setRequestVariables(requestVariables).setStreamTimeout(streamTimeout).setConnectTimeout(connectTimeout).setFailOnErrors(failOnErrors).setGrpcStencilUrl(defaultGrpcStencilUrl).setType(type).setRetainResponseType(retainResponseType).setHeaders(headerMap).setOutputMapping(outputMappings).setMetricId(metricId).setCapacity(capacity).createGrpcSourceConfig();
    }

    @Test
    public void shouldReturnConnectTimeout() {
        assertEquals(Integer.parseInt(connectTimeout), (int) grpcSourceConfig.getConnectTimeout());
    }

    @Test
    public void shouldReturnEndpoint() {
        assertEquals(endpoint, grpcSourceConfig.getEndpoint());
    }

    @Test
    public void shouldReturnStreamTimeout() {
        assertEquals(Integer.valueOf(streamTimeout), grpcSourceConfig.getStreamTimeout());
    }

    @Test
    public void shouldReturnBodyPattern() {
        assertEquals(requestPattern, grpcSourceConfig.getPattern());
    }

    @Test
    public void shouldReturnBodyVariable() {
        assertEquals(requestVariables, grpcSourceConfig.getVariables());
    }

    @Test
    public void isRetainResponseTypeShouldGetTheRightConfig() {
        assertEquals(retainResponseType, grpcSourceConfig.isRetainResponseType());
    }

    @Test
    public void shouldReturnFailOnErrors() {
        assertEquals(failOnErrors, grpcSourceConfig.isFailOnErrors());
    }

    @Test
    public void shouldReturnVerb() {
        assertEquals(grpcMethodUrl, grpcSourceConfig.getGrpcMethodUrl());
    }

    @Test
    public void getMetricIdShouldGetRightConfig() {
        assertEquals(metricId, grpcSourceConfig.getMetricId());
    }

    @Test
    public void shouldReturnType() {
        assertEquals(type, grpcSourceConfig.getType());
    }

    @Test
    public void hasTypeShouldBeTrueWhenTypeIsPresent() {
        assertTrue(grpcSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsNull() {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("", "", "", "", "", "", "", null, "", false, "", null, "", new HashMap<>(), new HashMap<>(), metricId, false);
        assertFalse(httpSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsEmpty() {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("", "",  "", "", "", "", "", "", "", false, "", "", "", new HashMap<>(), new HashMap<>(), metricId, false);
        assertFalse(httpSourceConfig.hasType());
    }

    @Test
    public void shouldReturnHeaderMap() {
        assertEquals(headerMap, grpcSourceConfig.getHeaders());
    }

    @Test
    public void shouldReturnOutputMapping() {
        assertEquals(outputMappings, grpcSourceConfig.getOutputMapping());
    }

    @Test
    public void shouldReturnCapacity() {
        assertEquals(capacity, grpcSourceConfig.getCapacity());
    }

    @Test
    public void shouldReturnColumnNames() {
        List<String> actualColumns = grpcSourceConfig.getOutputColumns();
        String[] expectedColumns = {"surge_factor"};
        assertArrayEquals(expectedColumns, actualColumns.toArray());
    }

    @Test
    public void shouldValidate() {
        grpcSourceConfig.validateFields();
    }

    @Test
    public void shouldCollectStencilURLSFromGRPCSourceConfiguration() {
        List<String> grpcStencilUrl = grpcSourceConfig.getGrpcStencilUrl();

        assertEquals(1, grpcStencilUrl.size());
        assertEquals("http://localhost/feast-proto/latest", grpcStencilUrl.get(0));
    }

    @Test
    public void shouldCollectMultipleStencilURLSFromGRPCSourceConfiguration() {
        defaultGrpcStencilUrl = "http://localhost/feast-proto/latest,http://localhost/log-entities/latest,http://localhost/events/latest,http://localhost/growth/release";
        grpcSourceConfig = new GrpcSourceConfigBuilder().setEndpoint(endpoint).setServicePort(servicePort).setGrpcRequestProtoSchema(grpcRequestProtoSchema).setGrpcResponseProtoSchema(grpcResponseProtoSchema).setGrpcMethodUrl(grpcMethodUrl).setRequestPattern(requestPattern).setRequestVariables(requestVariables).setStreamTimeout(streamTimeout).setConnectTimeout(connectTimeout).setFailOnErrors(failOnErrors).setGrpcStencilUrl(defaultGrpcStencilUrl).setType(type).setRetainResponseType(retainResponseType).setHeaders(headerMap).setOutputMapping(outputMappings).setMetricId(metricId).setCapacity(capacity).createGrpcSourceConfig();

        List<String> grpcStencilUrl = grpcSourceConfig.getGrpcStencilUrl();

        assertEquals(4, grpcStencilUrl.size());
        assertEquals("http://localhost/feast-proto/latest", grpcStencilUrl.get(0));
        assertEquals("http://localhost/log-entities/latest", grpcStencilUrl.get(1));
        assertEquals("http://localhost/events/latest", grpcStencilUrl.get(2));
        assertEquals("http://localhost/growth/release", grpcStencilUrl.get(3));
    }


    @Test
    public void shouldThrowExceptionIfSomeFieldsMissing() {

        grpcSourceConfig = new GrpcSourceConfigBuilder().setEndpoint(endpoint).setServicePort(servicePort).setGrpcRequestProtoSchema(grpcRequestProtoSchema).setGrpcResponseProtoSchema(grpcResponseProtoSchema).setGrpcMethodUrl(grpcMethodUrl).setRequestPattern(requestPattern).setRequestVariables(requestVariables).setStreamTimeout(null).setConnectTimeout(null).setFailOnErrors(failOnErrors).setGrpcStencilUrl(defaultGrpcStencilUrl).setType(type).setRetainResponseType(retainResponseType).setHeaders(headerMap).setOutputMapping(null).setMetricId("metricId_01").setCapacity(capacity).createGrpcSourceConfig();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> grpcSourceConfig.validateFields());
        assertEquals("Missing required fields: [streamTimeout, connectTimeout, outputMapping]", exception.getMessage());
    }

    @Test
    public void shouldReturnMandatoryFields() {
        HashMap<String, Object> expectedMandatoryFields = new HashMap<>();

        expectedMandatoryFields.put("endpoint", endpoint);
        expectedMandatoryFields.put("servicePort", servicePort);
        expectedMandatoryFields.put("grpcRequestProtoSchema", grpcRequestProtoSchema);
        expectedMandatoryFields.put("grpcResponseProtoSchema", grpcResponseProtoSchema);
        expectedMandatoryFields.put("grpcMethodUrl", grpcMethodUrl);
        expectedMandatoryFields.put("failOnErrors", failOnErrors);
        expectedMandatoryFields.put("requestPattern", requestPattern);
        expectedMandatoryFields.put("requestVariables", requestVariables);
        expectedMandatoryFields.put("streamTimeout", streamTimeout);
        expectedMandatoryFields.put("connectTimeout", connectTimeout);
        expectedMandatoryFields.put("outputMapping", outputMapping);

        HashMap<String, Object> actualMandatoryFields = grpcSourceConfig.getMandatoryFields();
        assertEquals(expectedMandatoryFields.get("endpoint"), actualMandatoryFields.get("endpoint"));
        assertEquals(expectedMandatoryFields.get("servicePort"), actualMandatoryFields.get("servicePort"));
        assertEquals(expectedMandatoryFields.get("grpcRequestProtoSchema"), actualMandatoryFields.get("grpcRequestProtoSchema"));
        assertEquals(expectedMandatoryFields.get("grpcResponseProtoSchema"), actualMandatoryFields.get("grpcResponseProtoSchema"));
        assertEquals(expectedMandatoryFields.get("grpcMethodUrl"), actualMandatoryFields.get("grpcMethodUrl"));
        assertEquals(expectedMandatoryFields.get("failOnErrors"), actualMandatoryFields.get("failOnErrors"));
        assertEquals(expectedMandatoryFields.get("capacity"), actualMandatoryFields.get("capacity"));
        assertEquals(expectedMandatoryFields.get("requestPattern"), actualMandatoryFields.get("requestPattern"));
        assertEquals(expectedMandatoryFields.get("requestVariables"), actualMandatoryFields.get("requestVariables"));
        assertEquals(expectedMandatoryFields.get("streamTimeout"), actualMandatoryFields.get("streamTimeout"));
        assertEquals(expectedMandatoryFields.get("connectTimeout"), actualMandatoryFields.get("connectTimeout"));
        assertEquals(outputMapping.getPath(), ((Map<String, OutputMapping>) actualMandatoryFields.get("outputMapping")).get("surge_factor").getPath());
    }

    @Test
    public void shouldValidateWhenOutputMappingIsEmpty() {

        grpcSourceConfig = new GrpcSourceConfigBuilder().setEndpoint(endpoint).setServicePort(servicePort).setGrpcRequestProtoSchema(grpcRequestProtoSchema).setGrpcResponseProtoSchema(grpcResponseProtoSchema).setGrpcMethodUrl(grpcMethodUrl).setRequestPattern(requestPattern).setRequestVariables(requestVariables).setStreamTimeout(streamTimeout).setConnectTimeout(connectTimeout).setFailOnErrors(failOnErrors).setGrpcStencilUrl(defaultGrpcStencilUrl).setType(type).setRetainResponseType(retainResponseType).setHeaders(headerMap).setOutputMapping(new HashMap<>()).setMetricId("metricId_01").setCapacity(capacity).createGrpcSourceConfig();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> grpcSourceConfig.validateFields());
        assertEquals("Missing required fields: [outputMapping]", exception.getMessage());
    }
}
