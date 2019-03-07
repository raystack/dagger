package com.gojek.daggers;

import com.gojek.daggers.async.connector.ESAsyncConnector;
import com.gojek.de.stencil.ClassLoadStencilClient;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import static com.gojek.daggers.Constants.*;
import static org.mockito.Mockito.*;

public class DeNormaliseStreamTest {

    @Test
    public void shouldAddSinkDirectlyWhenAsyncIsDisabled(){
        DataStream dataStream = mock(DataStream.class);
        Table table = mock(Table.class);
        when(table.getSchema()).thenReturn(mock(TableSchema.class));
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString("SINK_TYPE","influx")).thenReturn("log");
        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(false);
        DeNormaliseStream deNormaliseStream = new DeNormaliseStream(dataStream, configuration, table, new ClassLoadStencilClient());
        deNormaliseStream.apply();

        verify(dataStream).addSink(any(SinkFunction.class));
        verify(dataStream, never()).javaStream();
    }

    @Test
    public void shouldInteractWithResultStreamWhenAsyncIsEnabled(){
        DataStream dataStream = mock(DataStream.class);
        Table table = mock(Table.class);
        when(table.getSchema()).thenReturn(mock(TableSchema.class));
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString("SINK_TYPE","influx")).thenReturn("log");
        when(configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT)).thenReturn(true);
        when(configuration.getString(ASYNC_IO_STATSD_HOST_KEY,ASYNC_IO_STATSD_HOST_DEFAULT)).thenReturn("localhost");
        when(configuration.getString(ASYNC_IO_ES_HOST_KEY,ASYNC_IO_ES_HOST_DEFAULT)).thenReturn("localhost");

        org.apache.flink.streaming.api.datastream.DataStream resultStream = mock(org.apache.flink.streaming.api.datastream.DataStream.class);
        DeNormaliseStream deNormaliseStream = new DeNormaliseStreamStub(dataStream, configuration, table, new ClassLoadStencilClient(), resultStream);

        deNormaliseStream.apply();

        verify(resultStream).addSink(any(SinkFunction.class));
        verify(dataStream,never()).addSink(any(SinkFunction.class));
    }

    class DeNormaliseStreamStub extends DeNormaliseStream{

        private org.apache.flink.streaming.api.datastream.DataStream stream;

        public DeNormaliseStreamStub(DataStream<Row> dataStream, Configuration configuration, Table table, StencilClient stencilClient, org.apache.flink.streaming.api.datastream.DataStream stream) {
            super(dataStream, configuration, table, stencilClient);
            this.stream = stream;
        }

        @Override
        protected org.apache.flink.streaming.api.datastream.DataStream deNormalise(DataStream dataStream, ESAsyncConnector esConnector, int totalTimeout) {
            return stream;
        }
    }

}