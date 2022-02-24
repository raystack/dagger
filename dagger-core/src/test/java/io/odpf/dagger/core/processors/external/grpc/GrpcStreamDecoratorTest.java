package io.odpf.dagger.core.processors.external.grpc;

import io.odpf.dagger.core.processors.external.ExternalMetricConfig;
import io.odpf.dagger.core.processors.common.SchemaConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;


public class GrpcStreamDecoratorTest {
    @Mock
    private SchemaConfig schemaConfig;

    @Mock
    private GrpcSourceConfig grpcSourceConfig;

    @Mock
    private ExternalMetricConfig externalMetricConfig;

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void canDecorateGrpcAsync() {
        GrpcStreamDecorator grpcStreamDecorator = new GrpcStreamDecorator(grpcSourceConfig, externalMetricConfig, schemaConfig);
        assertTrue(grpcStreamDecorator.canDecorate());
    }

    @Test
    public void shouldNotDecorateOtherThanGrpcAsync() {
        GrpcStreamDecorator grpcStreamDecorator = new GrpcStreamDecorator(null, externalMetricConfig, schemaConfig);
        assertFalse(grpcStreamDecorator.canDecorate());
    }
}
