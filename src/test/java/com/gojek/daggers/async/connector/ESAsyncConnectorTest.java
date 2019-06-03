package com.gojek.daggers.async.connector;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.gojek.daggers.async.metric.AsyncAspects;
import com.gojek.daggers.utils.stats.StatsManager;
import com.gojek.de.stencil.StencilClient;
import com.gojek.esb.customer.CustomerLogMessage;
import com.google.protobuf.Descriptors;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ESAsyncConnectorTest {

    ResultFuture<Row> resultFuture;
    private ESAsyncConnector esAsyncConnector;
    private WireMockServer wireMockServer;
    private RuntimeContext runtimeContext;
    private Meter meter;
    private RestClient esClient;
    private Descriptors.Descriptor descriptor;
    private HashMap<String, String> configuration;


    @Before
    public void setUp() throws Exception {
        descriptor = CustomerLogMessage.getDescriptor();
        configuration = new HashMap<>();
        configuration.put("source", "es");
        configuration.put("host", "10.0.60.227: 9200,10.0.60.229: 9200,10.0.60.228:9200");
        configuration.put("type", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage");
        configuration.put("connect_timeout", "5000");
        configuration.put("path", "/drivers/driver/%s");
        configuration.put("retry_timeout", "5000");
        configuration.put("socket_timeout", "5000");
        configuration.put("stream_timeout", "5000");
        configuration.put("input_index", "5");
        configuration.put("field_name", "test");
        esClient = mock(RestClient.class);
        esAsyncConnector = new ESAsyncConnector(0, configuration, mock(StencilClient.class), esClient);
        resultFuture = mock(ResultFuture.class);
        wireMockServer = new WireMockServer(8081);
        wireMockServer.start();
        runtimeContext = mock(RuntimeContext.class);
        esAsyncConnector.setRuntimeContext(runtimeContext);
        MetricGroup metricGroup = mock(MetricGroup.class);
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(any())).thenReturn(metricGroup);
        meter = mock(Meter.class);
        when(metricGroup.meter(any(), any())).thenReturn(meter);
        esAsyncConnector.open(mock(Configuration.class));
    }

    @After
    public void tearDown() throws Exception {
        wireMockServer.stop();
    }

    @Test
    public void shouldNotModifyInputWhenEnrichmentKeyIsEmpty() throws Exception {
        Row input = new Row(1);
        Row value = mock(Row.class);
        input.setField(0, value);
        when(value.getField(5)).thenReturn(null);

        esAsyncConnector = new ESAsyncConnector(0, configuration, mock(StencilClient.class));
        esAsyncConnector.setRuntimeContext(runtimeContext);
        esAsyncConnector.open(mock(Configuration.class));
        esAsyncConnector.asyncInvoke(input, resultFuture);

        Row result = new Row(1);
        result.setField(0, value);
        verify(resultFuture, times(1)).complete(Collections.singleton(result));
    }

    @Test
    public void shouldEnrichInputForCorrespondingEnrichmentKey() throws Exception {
        Row input = new Row(1);
        Row value = mock(Row.class);
        input.setField(0, value);
        when(value.getField(5)).thenReturn("11223344545");

        esAsyncConnector.asyncInvoke(input, resultFuture);

        String groupName = "es.test";
        StatsManager statsManager = new StatsManager(runtimeContext, true);
        statsManager.register(groupName, AsyncAspects.values());
        EsResponseHandler esResponseHandler = new EsResponseHandler(input, resultFuture, descriptor, 0, statsManager);
        esResponseHandler.start();
        Request request = new Request("GET", "/drivers/driver/11223344545");
        verify(esClient, times(1)).performRequestAsync(eq(request), any(EsResponseHandler.class));
    }

    @Test
    public void shouldNotModifyInputOnTimeout() throws Exception {
        Row input = new Row(1);
        Row value = mock(Row.class);
        input.setField(0, value);
        esAsyncConnector.timeout(input, resultFuture);
        Row result = new Row(1);
        result.setField(0, value);
        verify(resultFuture, times(1)).complete(Collections.singleton(result));
    }

}
