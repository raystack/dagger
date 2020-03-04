package com.gojek.daggers.postProcessors.longbow.data;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.gojek.daggers.utils.Constants.*;

public class LongbowProtoData implements LongbowData {
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);

    public LongbowProtoData() {
    }

    @Override
    public Map<String, List<byte[]>> parse(List<Result> scanResult) {
        ArrayList<byte[]> data = new ArrayList<>();

        for (int i = 0; i < scanResult.size(); i++) {
            data.add(i, scanResult.get(i).getValue(COLUMN_FAMILY_NAME, Bytes.toBytes(LONGBOW_QUALIFIER_DEFAULT)));
        }

        HashMap<String, List<byte[]>> longbowData = new HashMap<>();
        longbowData.put(LONGBOW_PROTO_DATA, data);
        return longbowData;
    }
}
