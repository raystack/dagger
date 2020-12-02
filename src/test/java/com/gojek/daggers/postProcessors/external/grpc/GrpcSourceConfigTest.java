package com.gojek.daggers.postProcessors.external.grpc;


import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;
import com.google.gson.annotations.SerializedName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class GrpcSourceConfigTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    HashMap<String, String> headerMap;
    private GrpcSourceConfig grpcSourceConfig;
    private String endpoint;
    private int servicePort;
    private String requestPattern;
    private String requestVariables;
    private String streamTimeout;
    private String connectTimeout;
    private boolean failOnErrors;
    private String grpcStencilUrl;
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
        grpcStencilUrl = "http://stencil.golabs.io/artifactory/proto-descriptors/feast-proto/latest";
        type = "com.gojek.esb.booking.BookingLogMessage";
        capacity = 345;
        metricId = "metricId-http-01";
        retainResponseType = false;
        grpcMethodUrl = "com.esb.test/TestMethod";
        grpcResponseProtoSchema = "com.gojek.esb.response";
        grpcRequestProtoSchema = "com.gojek.esb.request";
        grpcSourceConfig = new GrpcSourceConfig(endpoint, servicePort, grpcRequestProtoSchema, grpcResponseProtoSchema, grpcMethodUrl, requestPattern, requestVariables, streamTimeout, connectTimeout, failOnErrors, grpcStencilUrl, type, retainResponseType, headerMap, outputMappings, metricId, capacity);
    }

    @Test
    public void shouldReturnConnectTimeout() {
        Assert.assertEquals(Integer.parseInt(connectTimeout), (int) grpcSourceConfig.getConnectTimeout());
    }

    @Test
    public void shouldReturnEndpoint() {
        Assert.assertEquals(endpoint, grpcSourceConfig.getEndpoint());
    }

    @Test
    public void shouldRe̵turnStreamTimeout() {
        Assert.assertEquals(Integer.valueOf(streamTimeout), grpcSourceConfig.getStreamTimeout());
    }

    @Test
    public void shouldReturnBodyPattern() {
        Assert.assertEquals(requestPattern, grpcSourceConfig.getPattern());
    }

    @Test
    public void shouldReturnBodyVariable() {
        Assert.assertEquals(requestVariables, grpcSourceConfig.getVariables());
    }

    @Test
    public void isRetainResponseTypeShouldGetTheRightConfig() {
        assertEquals(retainResponseType, grpcSourceConfig.isRetainResponseType());
    }

    @Test
    public void shouldReturnFailOnErrors() {
        Assert.assertEquals(failOnErrors, grpcSourceConfig.isFailOnErrors());
    }

    @Test
    public void shouldReturnVerb() {
        Assert.assertEquals(grpcMethodUrl, grpcSourceConfig.getGrpcMethodUrl());
    }

    @Test
    public void getMetricIdShouldGetRightConfig() {
        assertEquals(metricId, grpcSourceConfig.getMetricId());
    }

    @Test
    public void shouldReturnType() {
        Assert.assertEquals(type, grpcSourceConfig.getType());
    }

    @Test
    public void hasTypeShouldBeTrueWhenTypeIsPresent() {
        assertTrue(grpcSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsNull() {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("", "", "", "", null, "", false, null, "", new HashMap<>(), new HashMap<>(), metricId, false);
        assertFalse(httpSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsEmpty() {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("", "", "", "", "", "", false, "", "", new HashMap<>(), new HashMap<>(), metricId, false);
        assertFalse(httpSourceConfig.hasType());
    }

    @Test
    public void shouldReturnHeaderMap() {
        Assert.assertEquals(headerMap, grpcSourceConfig.getHeaders());
    }

    @Test
    public void shouldReturnOutputMapping() {
        Assert.assertEquals(outputMappings, grpcSourceConfig.getOutputMapping());
    }

    @Test
    public void shouldReturnCapacity() {
        Assert.assertEquals(capacity, grpcSourceConfig.getCapacity());
    }

    @Test
    public void shouldReturnColumnNames() {
        List<String> actualColumns = grpcSourceConfig.getOutputColumns();
        String[] expectedColumns = {"surge_factor"};
        Assert.assertArrayEquals(expectedColumns, actualColumns.toArray());
    }

    @Test
    public void shouldValidate() {
        expectedException = ExpectedException.none();

        grpcSourceConfig.validateFields();
    }

    @Test
    public void shouldCollectStencilURLSFromGRPCSourceConfiguration() {
        List<String> grpcStencilUrl = grpcSourceConfig.getGrpcStencilUrl();

        Assert.assertEquals(1, grpcStencilUrl.size());
        Assert.assertEquals("http://stencil.golabs.io/artifactory/proto-descriptors/feast-proto/latest", grpcStencilUrl.get(0));
    }

    @Test
    public void shouldCollectMultipleStencilURLSFromGRPCSourceConfiguration() {
        grpcStencilUrl = "http://stencil.golabs.io/artifactory/proto-descriptors/feast-proto/latest,http://artifactory-gojek.golabs.io/artifactory/proto-descriptors/esb-log-entities/latest,http://artifactory-gojek.golabs.io/artifactory/proto-descriptors/goid-events/latest,http://artifactory-gojek.golabs.io/artifactory/proto-descriptors/growth-log-entities/release";
        grpcSourceConfig = new GrpcSourceConfig(endpoint, servicePort, grpcRequestProtoSchema, grpcResponseProtoSchema, grpcMethodUrl, requestPattern, requestVariables, streamTimeout, connectTimeout, failOnErrors, grpcStencilUrl, type, retainResponseType, headerMap, outputMappings, metricId, capacity);

        List<String> grpcStencilUrl = grpcSourceConfig.getGrpcStencilUrl();

        Assert.assertEquals(4, grpcStencilUrl.size());
        Assert.assertEquals("http://stencil.golabs.io/artifactory/proto-descriptors/feast-proto/latest", grpcStencilUrl.get(0));
        Assert.assertEquals("http://artifactory-gojek.golabs.io/artifactory/proto-descriptors/esb-log-entities/latest", grpcStencilUrl.get(1));
        Assert.assertEquals("http://artifactory-gojek.golabs.io/artifactory/proto-descriptors/goid-events/latest", grpcStencilUrl.get(2));
        Assert.assertEquals("http://artifactory-gojek.golabs.io/artifactory/proto-descriptors/growth-log-entities/release", grpcStencilUrl.get(3));
    }


    @Test
    public void shouldThrowExceptionIfSomeFieldsMissing() {
        expectedException.expectMessage("Missing required fields: [streamTimeout, connectTimeout, outputMapping]");
        expectedException.expect(IllegalArgumentException.class);

        grpcSourceConfig = new GrpcSourceConfig(endpoint, servicePort, grpcRequestProtoSchema, grpcResponseProtoSchema, grpcMethodUrl, requestPattern, requestVariables, null, null, failOnErrors, grpcStencilUrl, type, retainResponseType, headerMap, null, "metricId_01", capacity);
        grpcSourceConfig.validateFields();
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
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing required fields: [outputMapping]");

        grpcSourceConfig = new GrpcSourceConfig(endpoint, servicePort, grpcRequestProtoSchema, grpcResponseProtoSchema, grpcMethodUrl, requestPattern, requestVariables, streamTimeout, connectTimeout, failOnErrors, grpcStencilUrl, type, retainResponseType, headerMap, new HashMap<>(), "metricId_01", capacity);

        grpcSourceConfig.validateFields();
    }
}
