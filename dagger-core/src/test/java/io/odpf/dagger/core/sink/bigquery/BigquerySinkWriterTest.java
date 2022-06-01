package io.odpf.dagger.core.sink.bigquery;


import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializerHelper;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class BigquerySinkWriterTest {

    @Test
    public void shouldWriteToOdpfSinkInBatches() throws IOException {
        ProtoSerializerHelper protoSerializerHelper = Mockito.mock(ProtoSerializerHelper.class);
        OdpfSink sink = Mockito.mock(OdpfSink.class);
        BigquerySinkWriter bigquerySinkWriter = new BigquerySinkWriter(protoSerializerHelper, sink, 3);
        Row row = new Row(1);
        row.setField(0, "some field");
        Mockito.when(protoSerializerHelper.serializeKey(row)).thenReturn("test".getBytes());
        Mockito.when(protoSerializerHelper.serializeValue(row)).thenReturn("testMessage".getBytes());
        OdpfSinkResponse response = Mockito.mock(OdpfSinkResponse.class);
        Mockito.when(response.hasErrors()).thenReturn(false);
        Mockito.when(sink.pushToSink(Mockito.anyList())).thenReturn(response);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        Mockito.verify(sink, Mockito.times(2)).pushToSink(Mockito.anyList());
        Mockito.verify(response, Mockito.times(2)).hasErrors();
    }

    @Test
    public void shouldNotWriteIfCurrentSizeIsLessThanTheBatchSize() throws IOException {
        ProtoSerializerHelper protoSerializerHelper = Mockito.mock(ProtoSerializerHelper.class);
        OdpfSink sink = Mockito.mock(OdpfSink.class);
        BigquerySinkWriter bigquerySinkWriter = new BigquerySinkWriter(protoSerializerHelper, sink, 10);
        Row row = new Row(1);
        row.setField(0, "some field");
        Mockito.when(protoSerializerHelper.serializeKey(row)).thenReturn("test".getBytes());
        Mockito.when(protoSerializerHelper.serializeValue(row)).thenReturn("testMessage".getBytes());
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        bigquerySinkWriter.write(row, null);
        Mockito.verify(sink, Mockito.times(0)).pushToSink(Mockito.anyList());
    }
}
