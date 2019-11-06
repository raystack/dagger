package com.gojek.daggers.postProcessors.external.es;

import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class EsStreamDecoratorTest {

    private StencilClient stencilClient;
    private EsSourceConfig esSourceConfig;


    @Before
    public void setUp(){
        stencilClient = mock(StencilClient.class);
        esSourceConfig = new EsSourceConfig("localhost", "9200", "",
                "driver_id", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", "5000", "5000", "5000", false, new HashMap<>());
    }

    @Test
    public void canDecorateStreamWhenConfigIsPresent() {
        EsStreamDecorator esStreamDecorator = new EsStreamDecorator(esSourceConfig, stencilClient, new ColumnNameManager(new String[4], new ArrayList<>()));

        Assert.assertTrue(esStreamDecorator.canDecorate());
    }

    @Test
    public void cannotDecorateStreamWhenConfigIsNull() {
        EsStreamDecorator esStreamDecorator = new EsStreamDecorator(null, stencilClient, new ColumnNameManager(new String[4], new ArrayList<>()));

        Assert.assertFalse(esStreamDecorator.canDecorate());
    }
}