package org.raystack.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class JsonUpdateTest {

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
        when(metricGroup.addGroup("udf", "JsonUpdate")).thenReturn(metricGroup);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        jsonUpdate.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }

    @Test
    public void shouldReturnAddedJsonStringForEmptyJsonWithNewKeyValue() {
        JsonUpdate jsonUpdate = new JsonUpdate();
        String jsonEvent = "{}";
        String expectedJsonEvent = "{\"k1\":\"v1\"}";
        String jPath = "$.k1";
        String updateValue = "v1";
        String actual = jsonUpdate.eval(jsonEvent, jPath, updateValue);
        Assert.assertEquals(expectedJsonEvent, actual);
    }

    @Test
    public void shouldReturnAddedJsonStringForNewKeyValuePair() {
        JsonUpdate jsonUpdate = new JsonUpdate();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String expectedJsonEvent = "{\"k1\":null,\"k2\":\"v2\",\"k3\":\"v3\"}";
        String jPath = "$.k3";
        String updateValue = "v3";
        Assert.assertEquals(expectedJsonEvent, jsonUpdate.eval(jsonEvent, jPath, updateValue));
    }

    @Test
    public void shouldReturnAddedJsonString() {
        JsonUpdate jsonUpdate = new JsonUpdate();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String expectedJsonEvent = "{\"k1\":null,\"k2\":\"updatedValue\"}";
        String jPath = "$.k2";
        String updateValue = "updatedValue";
        Assert.assertEquals(expectedJsonEvent, jsonUpdate.eval(jsonEvent, jPath, updateValue));
    }

    @Test
    public void shouldReturnAddedJsonStringForNestedJson() {
        JsonUpdate jsonUpdate = new JsonUpdate();
        String jsonEvent = "{\"k1\":null,\"k2\":{\"key1\":\"value1\",\"key2\":\"value2\"}}";
        String expectedJsonEvent = "{\"k1\":null,\"k2\":{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}}";
        String jPath = "$.k2.key3";
        String updateValue = "value3";
        String actual = jsonUpdate.eval(jsonEvent, jPath, updateValue);
        Assert.assertEquals(expectedJsonEvent, actual);
    }

    @Test
    public void shouldReturnUpdatedJsonStringForNestedJson() {
        JsonUpdate jsonUpdate = new JsonUpdate();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String expectedJsonEvent = "{\"k1\":null,\"k2\":{\"key1\":\"value1\",\"key2\":\"value2\"}}";
        String jPath = "$.k2";
        Map<String, String> updateValue = new HashMap<String, String>() {{
            put("key1", "value1");
            put("key2", "value2");
        }};
        Assert.assertEquals(expectedJsonEvent, jsonUpdate.eval(jsonEvent, jPath, updateValue));
    }

    @Test
    public void shouldReturnUpdatedJsonStringForNullValue() {
        JsonUpdate jsonUpdate = new JsonUpdate();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String expectedJsonEvent = "{\"k1\":null,\"k2\":null}";
        String jPath = "$.k2";
        String updateValue = null;
        Assert.assertEquals(expectedJsonEvent, jsonUpdate.eval(jsonEvent, jPath, updateValue));
    }

    @Test
    public void shouldReturnUpdatedJsonStringForNewNestedKeyValue() {
        JsonUpdate jsonUpdate = new JsonUpdate();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String expectedJsonEvent = "{\"k1\":null,\"k2\":\"v2\",\"k3\":{\"key1\":\"value1\",\"key2\":\"value2\"}}";
        String jPath = "$.k3";
        Map<String, String> updateValue = new HashMap<String, String>() {{
            put("key1", "value1");
            put("key2", "value2");
        }};
        String actual = jsonUpdate.eval(jsonEvent, jPath, updateValue);
        Assert.assertEquals(expectedJsonEvent, actual);
    }

    @Test
    public void shouldReturnUpdatedJsonStringForNewNestedKeyValueInNestedPath() {
        JsonUpdate jsonUpdate = new JsonUpdate();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\",\"k3\":{\"nk1\":\"nv1\",\"nk2\":\"nv2\"}}";
        String expectedJsonEvent = "{\"k1\":null,\"k2\":\"v2\",\"k3\":{\"nk1\":{\"key1\":\"value1\",\"key2\":\"value2\"},\"nk2\":\"nv2\"}}";
        String jPath = "$.k3.nk1";
        Map<String, String> updateValue = new HashMap<String, String>() {{
            put("key1", "value1");
            put("key2", "value2");
        }};
        String actual = jsonUpdate.eval(jsonEvent, jPath, updateValue);
        Assert.assertEquals(expectedJsonEvent, actual);
    }

    @Test
    public void shouldThrowErrorWhenNullJsonEvent() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("json string can not be null or empty");
        JsonUpdate jsonUpdate = new JsonUpdate();
        String jsonEvent = null;
        String jPath = "$.k2";
        String updateValue = "updatedValue";
        jsonUpdate.eval(jsonEvent, jPath, updateValue);
    }

    @Test
    public void shouldThrowErrorWhenNullJPath() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("json can not be null or empty");
        JsonUpdate jsonUpdate = new JsonUpdate();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String jPath = null;
        String updateValue = "updatedValue";
        jsonUpdate.eval(jsonEvent, jPath, updateValue);
    }

    @Test
    public void shouldThrowErrorWhenEmptyJPath() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("json can not be null or empty");
        JsonUpdate jsonUpdate = new JsonUpdate();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String jPath = "";
        String updateValue = "updatedValue";
        jsonUpdate.eval(jsonEvent, jPath, updateValue);
    }

    @Test
    public void shouldThrowErrorWhenImproperJPath() {
        thrown.expect(com.jayway.jsonpath.InvalidPathException.class);
        thrown.expectMessage("Illegal character at position 1 expected '.' or '[");
        JsonUpdate jsonUpdate = new JsonUpdate();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String jPath = "$ .k1";
        String updateValue = "updatedValue";
        jsonUpdate.eval(jsonEvent, jPath, updateValue);
    }

    @Test
    public void shouldThrowErrorWhenInvalidJPath() {
        thrown.expect(com.jayway.jsonpath.PathNotFoundException.class);
        JsonUpdate jsonUpdate = new JsonUpdate();
        String jsonEvent = "{\"k1\":null,\"k2\":\"v2\"}";
        String jPath = "$.k2.k4";
        String updateValue = "updatedValue";
        jsonUpdate.eval(jsonEvent, jPath, updateValue);
    }
    @Test
    public void shouldThrowErrorWhenInvalidJsonEvent() {
        thrown.expect(com.jayway.jsonpath.InvalidJsonException.class);
        JsonUpdate jsonUpdate = new JsonUpdate();
        String jsonEvent = "{\"k1\":null,\"k2\"\"v2\"}";
        String jPath = "$.k2";
        String updateValue = "updatedValue";
        jsonUpdate.eval(jsonEvent, jPath, updateValue);
    }
}
