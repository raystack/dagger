package io.odpf.dagger.core.processors.internal.processor.function;

import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.function.functions.CurrentTimestampFunction;
import io.odpf.dagger.core.processors.internal.processor.function.functions.InvalidFunction;
import io.odpf.dagger.core.processors.internal.processor.function.functions.JsonPayloadFunction;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FunctionInternalConfigProcessorTest {
    @Test
    public void shouldBeAbleToProcessFunctionCustomType() {
        ColumnNameManager columnManager = new ColumnNameManager(new String[0], new ArrayList<>());
        FunctionInternalConfigProcessor functionInternalConfigProcessor = new FunctionInternalConfigProcessor(columnManager, getCustomConfig("function"), null);

        assertTrue(functionInternalConfigProcessor.canProcess("function"));
    }

    @Test
    public void shouldNotBeAbleToProcessConstantCustomType() {
        ColumnNameManager columnManager = new ColumnNameManager(new String[0], new ArrayList<>());
        FunctionInternalConfigProcessor functionInternalConfigProcessor = new FunctionInternalConfigProcessor(columnManager, getCustomConfig("constant"), null);

        assertFalse(functionInternalConfigProcessor.canProcess("constant"));
    }

    @Test
    public void shouldNotBeAbleToProcessSqlCustomType() {
        ColumnNameManager columnManager = new ColumnNameManager(new String[0], new ArrayList<>());
        FunctionInternalConfigProcessor functionInternalConfigProcessor = new FunctionInternalConfigProcessor(columnManager, getCustomConfig("sql"), null);

        assertFalse(functionInternalConfigProcessor.canProcess("sql"));
    }

    @Test
    public void shouldGetCurrentTimestampFunctionProcessor() {
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("output2", "CURRENT_TIMESTAMP", "function", null);
        FunctionInternalConfigProcessor functionInternalConfigProcessor = new FunctionInternalConfigProcessor(null, internalSourceConfig, null);

        assertEquals(CurrentTimestampFunction.class, functionInternalConfigProcessor.functionProcessor.getClass());
    }

    @Test
    public void shouldGetJsonPayloadFunctionProcessor() {
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("output2", "JSON_PAYLOAD", "function", null);
        FunctionInternalConfigProcessor functionInternalConfigProcessor = new FunctionInternalConfigProcessor(null, internalSourceConfig, null);

        assertEquals(JsonPayloadFunction.class, functionInternalConfigProcessor.functionProcessor.getClass());
    }

    @Test
    public void shouldGetInvalidFunctionProcessor() {
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("output2", "SOME_INVALID_FUNCTION", "function", null);
        FunctionInternalConfigProcessor functionInternalConfigProcessor = new FunctionInternalConfigProcessor(null, internalSourceConfig, null);

        assertEquals(InvalidFunction.class, functionInternalConfigProcessor.functionProcessor.getClass());
    }

    @Test
    public void shouldThrowInvalidConfigurationException() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"input1", "input2"}, Arrays.asList("output1", "output2", "output3"));
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("output3", "test", "function", null);
        FunctionInternalConfigProcessor functionInternalConfigProcessor = new FunctionInternalConfigProcessor(columnNameManager, internalSourceConfig, null);

        Row inputRow = new Row(2);
        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        InvalidConfigurationException invalidConfigException = assertThrows(InvalidConfigurationException.class,
                () -> functionInternalConfigProcessor.process(rowManager));
        assertEquals("The function \"test\" is not supported in custom configuration",
                invalidConfigException.getMessage());
    }

    @Test
    public void shouldProcessToPopulateDataAtRightIndexForRightConfiguration() {
        long currentTimestampMs = System.currentTimeMillis();
        Clock clock = mock(Clock.class);
        when(clock.millis()).thenReturn(currentTimestampMs);

        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"input1", "input2"}, Arrays.asList("output1", "output2", "output3"));
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("output2", "CURRENT_TIMESTAMP", "function", null);
        FunctionInternalConfigProcessor functionInternalConfigProcessor = new FunctionInternalConfigProcessorMock(columnNameManager, internalSourceConfig, clock);

        Row inputRow = new Row(2);
        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        functionInternalConfigProcessor.process(rowManager);
        Timestamp expectedCurrentTimestamp = new Timestamp(currentTimestampMs);

        assertNotNull(rowManager.getOutputData().getField(1));
        assertEquals(expectedCurrentTimestamp, rowManager.getOutputData().getField(1));
    }

    private InternalSourceConfig getCustomConfig(String type) {
        return new InternalSourceConfig("field", "value", type, null);
    }

    final class FunctionInternalConfigProcessorMock extends FunctionInternalConfigProcessor {
        private FunctionInternalConfigProcessorMock(ColumnNameManager columnNameManager, InternalSourceConfig internalSourceConfig, Clock clock) {
            super(columnNameManager, internalSourceConfig, null);
            this.functionProcessor = new CurrentTimestampFunction(clock);
        }
    }
}
