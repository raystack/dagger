package io.odpf.dagger.functions.transformers;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StreamInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;


public class ClearColumnTransformerTest {

    @Mock
    private DataStream<Row> dataStream;

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldSetTargetColumnToEmpty() {
        Row inputRow = new Row(3);
        String[] columnNames = {"rule_id", "reason", "comms_data"};
        Map<String, String> transformationArguments = new HashMap<>();
        Map<String, String> commsData = new HashMap<>();
        commsData.put("wallet_id", "123");
        transformationArguments.put("targetColumnName", "reason");
        inputRow.setField(0, "NEWDEVICE.FREEZE.CR.UPDATE.PIN");
        inputRow.setField(1, "wallet-id-123");
        inputRow.setField(2, commsData);
        ClearColumnTransformer clearColumnTransformer = new ClearColumnTransformer(transformationArguments, columnNames, configuration);
        Row outputRow = clearColumnTransformer.map(inputRow);
        assertEquals("", outputRow.getField(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenTargetColumnIsNotPresent() {
        Row inputRow = new Row(3);
        String[] columnNames = {"rule_id", "reason", "comms_data"};
        Map<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("targetColumnName", "comms_data_1");
        inputRow.setField(0, "NEWDEVICE.FREEZE.CR.UPDATE.PIN");
        inputRow.setField(1, "wallet-id-123");
        inputRow.setField(2, "random");
        ClearColumnTransformer clearColumnTransformer = new ClearColumnTransformer(transformationArguments, columnNames, configuration);
        clearColumnTransformer.map(inputRow);
    }

    @Test
    public void shouldTransformInputStreamWithItsTransformer() {
        Row inputRow = new Row(3);
        String[] columnNames = {"rule_id", "reason", "comms_data"};
        Map<String, String> transformationArguments = new HashMap<>();
        Map<String, String> commsData = new HashMap<>();
        commsData.put("wallet_id", "123");
        transformationArguments.put("targetColumnName", "reason");
        inputRow.setField(0, "NEWDEVICE.FREEZE.CR.UPDATE.PIN");
        inputRow.setField(1, "wallet-id-123");
        inputRow.setField(2, commsData);
        ClearColumnTransformer clearColumnTransformer = new ClearColumnTransformer(transformationArguments, columnNames, configuration);
        StreamInfo inputStreamInfo = new StreamInfo(dataStream, columnNames);
        clearColumnTransformer.transform(inputStreamInfo);
        verify(dataStream, times(1)).map(any(ClearColumnTransformer.class));
    }

    @Test
    public void shouldReturnSameColumnNames() {
        Row inputRow = new Row(3);
        String[] columnNames = {"rule_id", "reason", "comms_data"};
        Map<String, String> transformationArguments = new HashMap<>();
        Map<String, String> commsData = new HashMap<>();
        commsData.put("wallet_id", "123");
        transformationArguments.put("targetColumnName", "reason");
        inputRow.setField(0, "NEWDEVICE.FREEZE.CR.UPDATE.PIN");
        inputRow.setField(1, "wallet-id-123");
        inputRow.setField(2, commsData);
        ClearColumnTransformer clearColumnTransformer = new ClearColumnTransformer(transformationArguments, columnNames, configuration);
        StreamInfo inputStreamInfo = new StreamInfo(dataStream, columnNames);
        StreamInfo outputStreamInfo = clearColumnTransformer.transform(inputStreamInfo);
        assertArrayEquals(columnNames, outputStreamInfo.getColumnNames());
    }
}
