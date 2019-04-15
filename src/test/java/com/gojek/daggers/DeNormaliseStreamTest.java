package com.gojek.daggers;

import com.gojek.daggers.decorator.EsStreamDecorator;
import com.gojek.daggers.decorator.StreamDecorator;
import com.gojek.daggers.decorator.StreamDecoratorFactory;
import com.gojek.daggers.decorator.TimestampDecorator;
import com.gojek.de.stencil.ClassLoadStencilClient;
import com.gojek.de.stencil.StencilClient;
import com.gojek.esb.fraud.EnrichedBookingLogMessage;
import mockit.Mock;
import mockit.MockUp;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.gojek.daggers.Constants.*;
import static org.mockito.Mockito.*;

public class DeNormaliseStreamTest {

    private Configuration configuration;
    private Table table;
    private DataStream dataStream;

    @Before
    public void setup() {
        dataStream = mock(DataStream.class);
        table = mock(Table.class);
        when(table.getSchema()).thenReturn(mock(TableSchema.class));
        configuration = mock(Configuration.class);
        when(configuration.getString("SINK_TYPE", "influx")).thenReturn("log");
    }

    @Test
    public void shouldAddSinkDirectlyWhenAsyncIsDisabled() {
        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(false);
        DeNormaliseStream deNormaliseStream = new DeNormaliseStream(dataStream, configuration, table, new ClassLoadStencilClient());
        deNormaliseStream.apply();

        verify(dataStream).addSink(any(SinkFunction.class));
        verify(dataStream, never()).javaStream();
    }

    @Test
    public void shouldCallResultStreamWhenAsyncIsEnabledWhenAllFieldsMappedInConfig() {
        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(true);
        when(configuration.getString(ASYNC_IO_ES_HOST_KEY, ASYNC_IO_ES_HOST_DEFAULT)).thenReturn("localhost");
        when(configuration.getString(OUTPUT_PROTO_CLASS_PREFIX_KEY, "")).thenReturn("com.gojek.esb.fraud.EnrichedBookingLog");
        when(configuration.getString(ASYNC_IO_KEY, "")).thenReturn("{\n" +
                "  \"booking_log\": {\n" +
                "    \"source\": \"input\"\n" +
                "  },\n" +
                "  \"customer_profile\": {\n" +
                "    \"source\": \"es\",\n" +
                "    \"host\": \"10.0.60.227: 9200, 10.0.60.229: 9200,10.0.60.228: 9200\",\n" +
                "    \"input_index\": \"5\",\n" +
                "    \"type\": \"com.gojek.esb.customer.CustomerLogMessage\",\n" +
                "    \"path\": \"/customers/customer/%s\",\n" +
                "    \"connect_timeout\": \"5000\",\n" +
                "    \"retry_timeout\": \"5000\",\n" +
                "    \"socket_timeout\": \"6000\",\n" +
                "    \"stream_timeout\": \"5000\"\n" +
                "  },\n" +
                "  \"driver_profile\": {\n" +
                "    \"source\": \"es\",\n" +
                "    \"host\": \"10.0.60.227: 9200,10.0.60.229: 9200,10.0.60.228: 9200\",\n" +
                "    \"input_index\": \"7\",\n" +
                "    \"type\": \"com.gojek.esb.fraud.DriverProfileFlattenLogMessage\",\n" +
                "    \"path\": \"/drivers/driver/%s\",\n" +
                "    \"connect_timeout\": \"5000\",\n" +
                "    \"retry_timeout\": \"5000\",\n" +
                "    \"socket_timeout\": \"5000\",\n" +
                "    \"stream_timeout\": \"6000\"\n" +
                "  },\n" +
                "  \"event_timestamp\": {\n" +
                "    \"source\": \"timestamp\"\n" +
                "  }\n" +
                "}");

        org.apache.flink.streaming.api.datastream.DataStream resultStream = mock(org.apache.flink.streaming.api.datastream.DataStream.class);
        StencilClient stencilClient = mock(StencilClient.class);
        when(stencilClient.get("com.gojek.esb.fraud.EnrichedBookingLogMessage")).thenReturn(EnrichedBookingLogMessage.getDescriptor());
        when(dataStream.javaStream()).thenReturn(resultStream);
        DeNormaliseStream deNormaliseStream = new DeNormaliseStream(dataStream, configuration, table, stencilClient);


        new MockUp<StreamDecoratorFactory>() {
            @Mock
            public StreamDecorator getStreamDecorator(Map<String, String> configuration, Integer fieldIndex, StencilClient stencilClient, Integer asyncIOCapacity, int outputProtoSize) {
                EsStreamDecorator mock = mock(EsStreamDecorator.class);
                when(mock.decorate(any())).thenReturn(resultStream);
                return mock;
            }
        };

        deNormaliseStream.apply();

        verify(resultStream).addSink(any(SinkFunction.class));
        verify(dataStream, never()).addSink(any(SinkFunction.class));
    }


    @Test
    public void shouldCallResultStreamWhenAsyncIsEnabledWhenOneFieldMappedInConfig() {
        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(true);
        when(configuration.getString(ASYNC_IO_ES_HOST_KEY, ASYNC_IO_ES_HOST_DEFAULT)).thenReturn("localhost");
        when(configuration.getString(OUTPUT_PROTO_CLASS_PREFIX_KEY, "")).thenReturn("com.gojek.esb.fraud.EnrichedBookingLog");
        when(configuration.getString(ASYNC_IO_KEY, "")).thenReturn("{\n" +
                "  \"event_timestamp\": {\n" +
                "    \"source\": \"timestamp\"\n" +
                "  }\n" +
                "}");

        org.apache.flink.streaming.api.datastream.DataStream resultStream = mock(org.apache.flink.streaming.api.datastream.DataStream.class);
        StencilClient stencilClient = mock(StencilClient.class);
        when(stencilClient.get("com.gojek.esb.fraud.EnrichedBookingLogMessage")).thenReturn(EnrichedBookingLogMessage.getDescriptor());
        when(dataStream.javaStream()).thenReturn(resultStream);
        DeNormaliseStream deNormaliseStream = new DeNormaliseStream(dataStream, configuration, table, stencilClient);


        new MockUp<StreamDecoratorFactory>() {
            @Mock
            public StreamDecorator getStreamDecorator(Map<String, String> configuration, Integer fieldIndex, StencilClient stencilClient, Integer asyncIOCapacity, int outputProtoSize) {
                TimestampDecorator mock = mock(TimestampDecorator.class);
                when(mock.decorate(any())).thenReturn(resultStream);
                return mock;
            }
        };

        deNormaliseStream.apply();

        verify(resultStream).addSink(any(SinkFunction.class));
        verify(dataStream, never()).addSink(any(SinkFunction.class));
    }

}