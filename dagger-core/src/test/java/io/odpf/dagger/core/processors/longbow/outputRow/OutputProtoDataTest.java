package io.odpf.dagger.core.processors.longbow.outputRow;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class OutputProtoDataTest {

    private LongbowSchema longbowSchema;
    private Row input;
    private Map<String, Object> scanResult;
    private byte[][] returnedProto;

    @Before
    public void setup() {
        String[] columnNames = {"longbow_read_key"};
        longbowSchema = new LongbowSchema(columnNames);
        returnedProto = Bytes.toByteArrays("mockedProtoByte");
        scanResult = new HashMap<>();
        scanResult.put("proto_data", returnedProto);
        input = new Row(1);
        input.setField(0, "driver#123");
    }

    @Test
    public void shouldAddOneForRowArity() {
        ReaderOutputProtoData outputProtoData = new ReaderOutputProtoData(longbowSchema);
        Row row = outputProtoData.get(scanResult, input);
        assertEquals(2, row.getArity());
    }

    @Test
    public void shouldAddProtoDataToRow() {
        ReaderOutputProtoData outputProtoData = new ReaderOutputProtoData(longbowSchema);
        Row row = outputProtoData.get(scanResult, input);
        Row expectedRow = Row.of("driver#123", returnedProto);
        assertEquals(expectedRow, row);
    }
}

