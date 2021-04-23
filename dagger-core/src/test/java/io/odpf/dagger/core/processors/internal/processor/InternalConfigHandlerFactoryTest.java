package io.odpf.dagger.core.processors.internal.processor;

import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.constant.ConstantInternalConfigProcessor;
import io.odpf.dagger.core.processors.internal.processor.function.FunctionInternalConfigProcessor;
import io.odpf.dagger.core.processors.internal.processor.invalid.InvalidInternalConfigProcessor;
import io.odpf.dagger.core.processors.internal.processor.sql.fields.SqlInternalConfigProcessor;
import org.junit.Assert;
import org.junit.Test;

public class InternalConfigHandlerFactoryTest {
    @Test
    public void shouldGetConstantInternalProcessor() {
        InternalConfigProcessor processor = InternalConfigHandlerFactory.getProcessor(new InternalSourceConfig("output_field", "value", "constant"), null, null);

        Assert.assertEquals(ConstantInternalConfigProcessor.class, processor.getClass());

    }

    @Test
    public void shouldGetFunctionInternalProcessor() {
        InternalConfigProcessor processor = InternalConfigHandlerFactory.getProcessor(new InternalSourceConfig("output_field", "functionValue", "function"), null, null);

        Assert.assertEquals(FunctionInternalConfigProcessor.class, processor.getClass());

    }

    @Test
    public void shouldGetSqlInternalProcessor() {
        InternalConfigProcessor processor = InternalConfigHandlerFactory.getProcessor(new InternalSourceConfig("output_field", "functionValue", "sql"), null, null);

        Assert.assertEquals(SqlInternalConfigProcessor.class, processor.getClass());

    }

    @Test
    public void shouldGetInvalidInternalProcessor() {
        InternalConfigProcessor processor = InternalConfigHandlerFactory.getProcessor(new InternalSourceConfig("output_field", "functionValue", "invalid"), null, null);

        Assert.assertEquals(InvalidInternalConfigProcessor.class, processor.getClass());

    }

}
