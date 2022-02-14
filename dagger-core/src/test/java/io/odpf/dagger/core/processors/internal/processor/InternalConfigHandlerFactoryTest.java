package io.odpf.dagger.core.processors.internal.processor;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.constant.ConstantInternalConfigProcessor;
import io.odpf.dagger.core.processors.internal.processor.function.FunctionInternalConfigProcessor;
import io.odpf.dagger.core.processors.internal.processor.invalid.InvalidInternalConfigProcessor;
import io.odpf.dagger.core.processors.internal.processor.sql.fields.SqlInternalConfigProcessor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class InternalConfigHandlerFactoryTest {
    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getString("STREAMS", ""))
                .thenReturn("[{\"INPUT_SCHEMA_PROTO_CLASS\": \"io.odpf.dagger.consumer.TestBookingLogMessage\"}]");
        when(configuration.getBoolean("SCHEMA_REGISTRY_STENCIL_ENABLE", false))
                .thenReturn(false);
        when(configuration.getString("SCHEMA_REGISTRY_STENCIL_URLS", ""))
                .thenReturn("");
    }

    @Test
    public void shouldGetConstantInternalProcessor() {
        InternalConfigProcessor processor = InternalConfigHandlerFactory.getProcessor(new InternalSourceConfig("output_field", "value", "constant"), null, null, configuration);

        assertEquals(ConstantInternalConfigProcessor.class, processor.getClass());
    }

    @Test
    public void shouldGetFunctionInternalProcessor() {
        InternalConfigProcessor processor = InternalConfigHandlerFactory.getProcessor(new InternalSourceConfig("output_field", "functionValue", "function"), null, null, configuration);

        assertEquals(FunctionInternalConfigProcessor.class, processor.getClass());
    }

    @Test
    public void shouldGetSqlInternalProcessor() {
        InternalConfigProcessor processor = InternalConfigHandlerFactory.getProcessor(new InternalSourceConfig("output_field", "functionValue", "sql"), null, null, configuration);

        assertEquals(SqlInternalConfigProcessor.class, processor.getClass());
    }

    @Test
    public void shouldGetInvalidInternalProcessor() {
        InternalConfigProcessor processor = InternalConfigHandlerFactory.getProcessor(new InternalSourceConfig("output_field", "functionValue", "invalid"), null, null, configuration);

        assertEquals(InvalidInternalConfigProcessor.class, processor.getClass());
    }

    @Test
    public void shouldThrowInvalidConfigurationExceptionWhenInvalidDaggerConfigProvided() {
        Configuration invalidConfiguration = mock(Configuration.class);
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("output_field", "functionValue", "sql");

        InvalidConfigurationException invalidConfigException = assertThrows(InvalidConfigurationException.class, () -> InternalConfigHandlerFactory.getProcessor(internalSourceConfig, null, null, invalidConfiguration));
        assertEquals("Invalid configuration: STREAMS not provided",
                invalidConfigException.getMessage());
    }
}
