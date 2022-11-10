package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class JsonQueryTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "JsonQuery")).thenReturn(metricGroup);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        JsonQuery jsonQuery = new JsonQuery();
        jsonQuery.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }

    @Test
    public void shouldReturnJsonString() throws JsonProcessingException {
        JsonQuery jsonQuery = new JsonQuery();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String expectedJsonEvent = "\"v2\"";
        String jPath = "$.k2";
        Assert.assertEquals(expectedJsonEvent, jsonQuery.eval(jsonEvent, jPath));
    }

    @Test
    public void shouldReturnJsonStringForNestedJson() throws JsonProcessingException {
        JsonQuery jsonQuery = new JsonQuery();
        String expectedJsonEvent = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
        String jsonEvent = "{\"k1\":null,\"k2\":{\"key1\":\"value1\",\"key2\":\"value2\"}}";
        String jPath = "$.k2";
        Assert.assertEquals(expectedJsonEvent, jsonQuery.eval(jsonEvent, jPath));
    }

    @Test
    public void shouldReturnJsonStringForNullValue() throws JsonProcessingException {
        JsonQuery jsonQuery = new JsonQuery();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String jPath = "$.k1";
        String result = jsonQuery.eval(jsonEvent, jPath);
        Assert.assertNull(result);
    }

    @Test
    public void shouldThrowErrorWhenNullJsonEvent() throws JsonProcessingException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("json string can not be null or empty");
        JsonQuery jsonQuery = new JsonQuery();
        String jsonEvent = null;
        String jPath = "$.k2";
        jsonQuery.eval(jsonEvent, jPath);
    }

    @Test
    public void shouldThrowErrorWhenNullJPath() throws JsonProcessingException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("json can not be null or empty");
        JsonQuery jsonQuery = new JsonQuery();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String jPath = null;
        jsonQuery.eval(jsonEvent, jPath);
    }

    @Test
    public void shouldThrowErrorWhenEmptyJPath() throws JsonProcessingException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("json can not be null or empty");
        JsonQuery jsonQuery = new JsonQuery();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String jPath = "";
        jsonQuery.eval(jsonEvent, jPath);
    }

    @Test
    public void shouldThrowErrorWhenImproperJPath() throws JsonProcessingException {
        thrown.expect(com.jayway.jsonpath.InvalidPathException.class);
        thrown.expectMessage("Illegal character at position 1 expected '.' or '[");
        JsonQuery jsonQuery = new JsonQuery();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String jPath = "$ .k1";
        jsonQuery.eval(jsonEvent, jPath);
    }

    @Test
    public void shouldThrowErrorWhenInvalidJPath() throws JsonProcessingException {
        thrown.expect(com.jayway.jsonpath.PathNotFoundException.class);
        JsonQuery jsonQuery = new JsonQuery();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String jPath = "$.k2.k4";
        jsonQuery.eval(jsonEvent, jPath);
    }
}
