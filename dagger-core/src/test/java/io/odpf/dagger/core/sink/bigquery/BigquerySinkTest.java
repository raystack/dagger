package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializer;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.bigquery.BigQuerySinkFactory;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Optional;

public class BigquerySinkTest {

    @Test
    public void shouldReturnCommittersAndSerializer() throws IOException {
        ProtoSerializer protoSerializer = Mockito.mock(ProtoSerializer.class);
        BigQuerySinkFactory sinkFactory = Mockito.mock(BigQuerySinkFactory.class);
        BigquerySink sink = new BigquerySink(222, protoSerializer, sinkFactory);
        Assert.assertEquals(sinkFactory, sink.getSinkFactory());
        Assert.assertEquals(222, sink.getBatchSize());
        Assert.assertEquals(Optional.empty(), sink.createCommitter());
        Assert.assertEquals(Optional.empty(), sink.getWriterStateSerializer());
        Assert.assertEquals(Optional.empty(), sink.createGlobalCommitter());
        Assert.assertEquals(Optional.empty(), sink.getCommittableSerializer());
        Assert.assertEquals(Optional.empty(), sink.getGlobalCommittableSerializer());
    }

    @Test
    public void voidShouldCreateSinkWriter() throws IOException {
        ProtoSerializer protoSerializer = Mockito.mock(ProtoSerializer.class);
        BigQuerySinkFactory sinkFactory = Mockito.mock(BigQuerySinkFactory.class);
        OdpfSink odpfSink = Mockito.mock(OdpfSink.class);
        Mockito.when(sinkFactory.create()).thenReturn(odpfSink);
        BigquerySink sink = new BigquerySink(222, protoSerializer, sinkFactory);
        SinkWriter<Row, Void, Void> writer = sink.createWriter(null, null);
        Assert.assertTrue(writer instanceof BigquerySinkWriter);
        Mockito.verify(sinkFactory, Mockito.times(1)).create();
    }
}
