package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
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

public class JsonDeleteTest {

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
        when(metricGroup.addGroup("udf", "JsonDelete")).thenReturn(metricGroup);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        JsonDelete jsonDelete = new JsonDelete();
        jsonDelete.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }

    @Test
    public void shouldReturnDeletedValueAsJsonString() {
        JsonDelete jsonDelete = new JsonDelete();
        String jsonEvent = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
        String expectedJsonEvent = "{\"k1\":\"v1\"}";
        String jPath = "$.k2";
        String actual = jsonDelete.eval(jsonEvent, jPath);
        Assert.assertEquals(expectedJsonEvent, actual);
    }

    @Test
    public void shouldReturnDeletedNodeAsJsonString() {
        JsonDelete jsonDelete = new JsonDelete();
        String jsonEvent = "{\"k1\":\"v1\",\"k2\":{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}}";
        String expectedJsonEvent = "{\"k1\":\"v1\"}";
        String jPath = "$.k2";
        String actual = jsonDelete.eval(jsonEvent, jPath);
        Assert.assertEquals(expectedJsonEvent, actual);
    }

    @Test
    public void shouldReturnDeletedNestedValueAsJsonString() {
        JsonDelete jsonDelete = new JsonDelete();
        String jsonEvent = "{\"k1\":\"v1\",\"k2\":{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}}";
        String expectedJsonEvent = "{\"k1\":\"v1\",\"k2\":{\"key2\":\"value2\",\"key3\":\"value3\"}}";
        String jPath = "$.k2.key1";
        String actual = jsonDelete.eval(jsonEvent, jPath);
        Assert.assertEquals(expectedJsonEvent, actual);
    }

    @Test
    public void shouldReturnDeletedArrayValueAsJsonString() {
        JsonDelete jsonDelete = new JsonDelete();
        String jsonEvent = "{\"k1\":\"v1\",\"k2\":[\"value1\",\"value2\",\"value3\"]}";
        String expectedJsonEvent = "{\"k1\":\"v1\",\"k2\":[\"value1\",\"value3\"]}";
        String jPath = "$.k2[1]";
        String actual = jsonDelete.eval(jsonEvent, jPath);
        Assert.assertEquals(expectedJsonEvent, actual);
    }

    @Test
    public void shouldReturnEmptyJsonAsJsonString() {
        JsonDelete jsonDelete = new JsonDelete();
        String jsonEvent = "{\"k1\":\"v1\",\"k2\":[\"value1\",\"value2\",\"value3\"]}";
        String expectedJsonEvent = "{}";
        String jPath = "$..*";
        String actual = jsonDelete.eval(jsonEvent, jPath);
        Assert.assertEquals(expectedJsonEvent, actual);
    }

    @Test
    public void shouldThrowErrorWhenNullJsonEvent() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("json string can not be null or empty");
        JsonDelete jsonDelete = new JsonDelete();
        String jsonEvent = null;
        String jPath = "$.k2";
        jsonDelete.eval(jsonEvent, jPath);
    }

    @Test
    public void shouldThrowErrorWhenNullJPath() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("json can not be null or empty");
        JsonDelete jsonDelete = new JsonDelete();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String jPath = null;
        jsonDelete.eval(jsonEvent, jPath);
    }

    @Test
    public void shouldThrowErrorWhenEmptyJPath() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("json can not be null or empty");
        JsonDelete jsonDelete = new JsonDelete();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String jPath = "";
        jsonDelete.eval(jsonEvent, jPath);
    }

    @Test
    public void shouldThrowErrorWhenImproperJPath() {
        thrown.expect(com.jayway.jsonpath.InvalidPathException.class);
        thrown.expectMessage("Illegal character at position 1 expected '.' or '[");
        JsonDelete jsonDelete = new JsonDelete();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String jPath = "$ .k1";
        jsonDelete.eval(jsonEvent, jPath);
    }

    @Test
    public void shouldThrowErrorWhenInvalidJPath() {
        thrown.expect(com.jayway.jsonpath.PathNotFoundException.class);
        JsonDelete jsonDelete = new JsonDelete();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String jPath = "$.k2.k4";
        jsonDelete.eval(jsonEvent, jPath);
    }

    @Test
    public void shouldThrowErrorWhenInvalidJsonEvent() {
        thrown.expect(com.jayway.jsonpath.InvalidJsonException.class);
        JsonDelete jsonDelete = new JsonDelete();
        String jsonEvent = "{\"k1\":null,\"k2\"\"v2\"}";
        String jPath = "$.k2";
        jsonDelete.eval(jsonEvent, jPath);
    }
}
