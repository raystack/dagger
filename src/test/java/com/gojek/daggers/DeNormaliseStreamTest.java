package com.gojek.daggers;

import com.gojek.de.stencil.ClassLoadStencilClient;
import com.gojek.de.stencil.StencilClient;
import com.gojek.esb.fraud.EnrichedBookingLogMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Ignore;
import org.junit.Test;

import static com.gojek.daggers.Constants.*;
import static org.mockito.Mockito.*;

public class DeNormaliseStreamTest {

    @Test
    public void shouldAddSinkDirectlyWhenAsyncIsDisabled() {
        DataStream dataStream = mock(DataStream.class);
        Table table = mock(Table.class);
        when(table.getSchema()).thenReturn(mock(TableSchema.class));
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString("SINK_TYPE", "influx")).thenReturn("log");
        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(false);
        DeNormaliseStream deNormaliseStream = new DeNormaliseStream(dataStream, configuration, table, new ClassLoadStencilClient());
        deNormaliseStream.apply();

        verify(dataStream).addSink(any(SinkFunction.class));
        verify(dataStream, never()).javaStream();
    }

    @Test
    @Ignore
    public void shouldInteractWithResultStreamWhenAsyncIsEnabled() {
        DataStream dataStream = mock(DataStream.class);
        Table table = mock(Table.class);
        when(table.getSchema()).thenReturn(mock(TableSchema.class));
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString("SINK_TYPE", "influx")).thenReturn("log");
        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(true);
        when(configuration.getString(ASYNC_IO_STATSD_HOST_KEY, ASYNC_IO_STATSD_HOST_DEFAULT)).thenReturn("localhost");
        when(configuration.getString(ASYNC_IO_ES_HOST_KEY, ASYNC_IO_ES_HOST_DEFAULT)).thenReturn("localhost");
        when(configuration.getString(OUTPUT_PROTO_CLASS_PREFIX_KEY, "")).thenReturn("com.gojek.esb.fraud.EnrichedBookingLog");
        when(configuration.getInteger(ASYNC_IO_CAPCITY_KEY, ASYNC_IO_CAPCITY_DEFAULT)).thenReturn(30);
        when(configuration.getString(ASYNC_IO_KEY, "")).thenReturn("{\n" +
                "  \"booking_log_message\": { \n" +
                "  \"source\": \"input\"\n" +
                "  },\n" +
                "  \"customer_profile_message\": { \n" +
                "    \"source\" : \"es\",\n" +
                "    \"host\": \"10.120.2.212:9200,10.120.2.218:9200,10.120.2.211:9200\",\n" +
                "    \"key\": \"customer_id\",\n" +
                "    \"input_index\": \"5\",\n" +
                "    \"type\": \"com.gojek.esb.customer.CustomerLogMessage\",\n" +
                "    \"path\":\"/customers/customer/%s\",\n" +
                "    \"connect_timeout\":\"5000\",\n" +
                "    \"retry_timeout\":\"5000\",\n" +
                "    \"socket_timeout\": \"5000\"\n" +
                "  },\n" +
                "  \"event_timestamp\" : {\n" +
                "    \"source\" : \"timestamp\"\n" +
                "  }\n" +
                "}");

        org.apache.flink.streaming.api.datastream.DataStream resultStream = mock(org.apache.flink.streaming.api.datastream.DataStream.class);
        StencilClient stencilClient = mock(StencilClient.class);
        when(stencilClient.get("com.gojek.esb.fraud.EnrichedBookingLogMessage")).thenReturn(EnrichedBookingLogMessage.getDescriptor());
        when(dataStream.javaStream()).thenReturn(resultStream);
        DeNormaliseStream deNormaliseStream = new DeNormaliseStreamStub(dataStream, configuration, table, stencilClient);

        deNormaliseStream.apply();

        verify(resultStream).addSink(any(SinkFunction.class));
        verify(dataStream, never()).addSink(any(SinkFunction.class));
    }

    class DeNormaliseStreamStub extends DeNormaliseStream {

        public DeNormaliseStreamStub(DataStream<Row> dataStream, Configuration configuration, Table table, StencilClient stencilClient) {
            super(dataStream, configuration, table, stencilClient);
        }
    }

}