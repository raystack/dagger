package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;


public class BigQuerySinkBuilderTest {

    @Test
    public void shouldBuildBigQuerySink() {
        StencilClientOrchestrator stencilClientOrchestrator = Mockito.mock(StencilClientOrchestrator.class);
        BigQuerySinkBuilder builder = BigQuerySinkBuilder.create();
        builder.setColumnNames(new String[]{"test", "some_column"});
        builder.setConfiguration(new Configuration(ParameterTool.fromMap(new HashMap<String, String>() {{
            put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "test");
        }})));
        builder.setStencilClientOrchestrator(stencilClientOrchestrator);
        Assert.assertNotNull(builder.build());
    }
}
