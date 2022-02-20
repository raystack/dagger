package io.odpf.dagger.core.processors.internal.processor.function;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

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

    @Ignore("again, should be moved to JsonPayloadFunction tests")
    @Test
    public void shouldThrowInvalidConfigurationExceptionWhenInvalidDaggerConfigProvided() {
        Configuration testConfiguration = mock(Configuration.class);
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"input1", "input2"}, Arrays.asList("output1", "output2", "output3"));
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("output3", "test", "function", null);

        InvalidConfigurationException invalidConfigException = assertThrows(InvalidConfigurationException.class, () -> {
            new FunctionInternalConfigProcessor(columnNameManager, internalSourceConfig, null);
        });
        assertEquals("Invalid configuration: STREAMS not provided",
                invalidConfigException.getMessage());
    }

    @Test
    public void shouldProcessToPopulateDataAtRightIndexForRightConfiguration() {
        Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"input1", "input2"}, Arrays.asList("output1", "output2", "output3"));
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("output2", "CURRENT_TIMESTAMP", "function", null);
        FunctionInternalConfigProcessor functionInternalConfigProcessor = new FunctionInternalConfigProcessorMock(columnNameManager, internalSourceConfig, currentTimestamp);

        Row inputRow = new Row(2);
        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        functionInternalConfigProcessor.process(rowManager);

        assertNotNull(rowManager.getOutputData().getField(1));
        assertEquals(currentTimestamp, rowManager.getOutputData().getField(1));
    }

    private InternalSourceConfig getCustomConfig(String type) {
        return new InternalSourceConfig("field", "value", type, null);
    }

    final class FunctionInternalConfigProcessorMock extends FunctionInternalConfigProcessor {
        private FunctionInternalConfigProcessorMock(ColumnNameManager columnNameManager, InternalSourceConfig internalSourceConfig, Timestamp currentTimestamp) {
            super(columnNameManager, internalSourceConfig, null);
            this.functionProcessor = new FunctionProcessor(currentTimestamp);
        }
    }

    final class FunctionProcessor implements io.odpf.dagger.core.processors.internal.processor.function.FunctionProcessor {
        private Timestamp currentTimestamp;

        private FunctionProcessor(Timestamp currentTimestamp) {
            this.currentTimestamp = currentTimestamp;
        }

        @Override
        public boolean canProcess(String functionName) {
            return true;
        }

        @Override
        public Object getResult(RowManager rowManager) {
            return currentTimestamp;
        }
    }
}
