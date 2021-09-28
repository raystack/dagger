package io.odpf.dagger.core.processors.longbow.data;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.*;

import static io.odpf.dagger.core.utils.Constants.LONGBOW_COLUMN_FAMILY_DEFAULT;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowTableDataTest {

    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);

    @Mock
    private Result result1;

    @Mock
    private Result result2;

    @Before
    public void setUp() {
        initMocks(this);
        when(result1.getValue(COLUMN_FAMILY_NAME, Bytes.toBytes("longbow_data1"))).thenReturn(Bytes.toBytes("RB-234"));
        when(result1.getValue(COLUMN_FAMILY_NAME, Bytes.toBytes("longbow_data2"))).thenReturn(Bytes.toBytes("RB-235"));
        when(result2.getValue(COLUMN_FAMILY_NAME, Bytes.toBytes("longbow_data1"))).thenReturn(Bytes.toBytes("RB-224"));
        when(result2.getValue(COLUMN_FAMILY_NAME, Bytes.toBytes("longbow_data2"))).thenReturn(Bytes.toBytes("RB-225"));
    }

    @Test
    public void shouldReturnEmptyDataWhenScanResultIsEmpty() {
        List<Result> scanResult = new ArrayList<>();
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration"};

        LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        LongbowTableData longbowTableData = new LongbowTableData(longbowSchema);
        Map<String, List<String>> actualData = longbowTableData.parse(scanResult);
        assertEquals(Collections.emptyList(), actualData.get("longbow_data1"));
    }

    @Test
    public void shouldReturnListOfString() {
        List<Result> scanResult = new ArrayList<>();
        scanResult.add(result1);
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration"};

        LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        LongbowTableData longbowTableData = new LongbowTableData(longbowSchema);
        Map<String, List<String>> actualData = longbowTableData.parse(scanResult);
        assertEquals(Collections.singletonList("RB-234"), actualData.get("longbow_data1"));
    }

    @Test
    public void shouldReturnMultipleListOfStringWhenLongbowDataMoreThanOne() {
        List<Result> scanResult = new ArrayList<>();
        scanResult.add(result1);
        scanResult.add(result2);
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration", "longbow_data2"};

        LongbowSchema longbowSchema = new LongbowSchema(columnNames);
        LongbowTableData longbowTableData = new LongbowTableData(longbowSchema);
        Map<String, List<String>> actualData = longbowTableData.parse(scanResult);
        Map<String, List<String>> expectedMap = new HashMap<String, List<String>>() {{
            put("longbow_data1", Arrays.asList("RB-234", "RB-224"));
            put("longbow_data2", Arrays.asList("RB-235", "RB-225"));
        }};
        assertEquals(expectedMap, actualData);
    }
}
