package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;


public class BigquerySinkBuilderTest {

    @Test
    public void shouldBuildBigquerySink() {
        BigquerySinkBuilder builder = BigquerySinkBuilder.create();
        builder.setColumnNames(new String[]{"test", "some_column"});
        Configuration conf = new Configuration(ParameterTool.fromMap(new HashMap<String, String>() {{
            put("a", "b");
            put("SINK_BIGQUERY_BATCH_SIZE", "222");
        }}));
        builder.setConfiguration(conf);
        builder.setKeyProtoClassName("test.key.class");
        builder.setMessageProtoClassName("test.message.class");
        StencilClientOrchestrator stencilClientOrchestrator = Mockito.mock(StencilClientOrchestrator.class);
        builder.setStencilClientOrchestrator(stencilClientOrchestrator);

        BigquerySink sink = builder.build();
        Assert.assertEquals(conf, sink.getConfiguration());
        Assert.assertEquals(222, sink.getBatchSize());
    }

}
