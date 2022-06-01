package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializerHelper;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;

public class BigquerySinkTest {

    @Test
    public void shouldReturnCommittersAndSerializer() throws IOException {
        Configuration conf = new Configuration(ParameterTool.fromMap(new HashMap<String, String>() {{
            put("a", "b");
            put("SINK_BIGQUERY_BATCH_SIZE", "222");
        }}));
        ProtoSerializerHelper protoSerializerHelper = Mockito.mock(ProtoSerializerHelper.class);
        BigquerySink sink = new BigquerySink(protoSerializerHelper, conf);
        Assert.assertEquals(conf, sink.getConfiguration());
        Assert.assertEquals(222, sink.getBatchSize());
        Assert.assertEquals(Optional.empty(), sink.createCommitter());
        Assert.assertEquals(Optional.empty(), sink.getWriterStateSerializer());
        Assert.assertEquals(Optional.empty(), sink.createGlobalCommitter());
        Assert.assertEquals(Optional.empty(), sink.getCommittableSerializer());
        Assert.assertEquals(Optional.empty(), sink.getGlobalCommittableSerializer());
    }
}
