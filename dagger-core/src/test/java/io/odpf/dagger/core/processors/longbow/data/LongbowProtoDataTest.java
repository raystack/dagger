package io.odpf.dagger.core.processors.longbow.data;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.*;

import static io.odpf.dagger.core.utils.Constants.LONGBOW_COLUMN_FAMILY_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.LONGBOW_QUALIFIER_DEFAULT;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowProtoDataTest {

    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);

    @Mock
    private Result scanResult;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldParseProtoByteDataFromBigTable() {
        ArrayList<Result> results = new ArrayList<>();
        results.add(scanResult);
        byte[] mockResult = Bytes.toBytes("test");
        when(scanResult.getValue(COLUMN_FAMILY_NAME, Bytes.toBytes(LONGBOW_QUALIFIER_DEFAULT))).thenReturn(mockResult);
        LongbowProtoData longbowProtoData = new LongbowProtoData();
        Map<String, List<byte[]>> actualMap = longbowProtoData.parse(results);
        Map<String, List<byte[]>> expectedMap = new HashMap<String, List<byte[]>>() {{
            put("proto_data", Arrays.asList(mockResult));
        }};
        assertEquals(expectedMap, actualMap);
    }
}
