package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.depot.bigquery.BigQuerySinkFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


public class BigquerySinkBuilderTest {

    @Test
    public void shouldBuildBigquerySink() {
        BigquerySinkBuilder builder = BigquerySinkBuilder.create();
        builder.setColumnNames(new String[]{"test", "some_column"});
        BigQuerySinkFactory sinkFactory = Mockito.mock(BigQuerySinkFactory.class);
        builder.setSinkConnectorFactory(sinkFactory);
        builder.setSchemaKeyClass("test.key.class");
        builder.setBatchSize(222);
        builder.setSchemaMessageClass("test.message.class");
        StencilClientOrchestrator stencilClientOrchestrator = Mockito.mock(StencilClientOrchestrator.class);
        builder.setStencilClientOrchestrator(stencilClientOrchestrator);

        BigquerySink sink = builder.build();
        Assert.assertEquals(sinkFactory, sink.getSinkFactory());
        Assert.assertEquals(222, sink.getBatchSize());
    }
}
