package io.odpf.dagger.core.processors.longbow.data;

import io.odpf.dagger.core.utils.Constants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Longbow proto data.
 */
public class LongbowProtoData implements LongbowData {
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(Constants.LONGBOW_COLUMN_FAMILY_DEFAULT);

    /**
     * Instantiates a new Longbow proto data.
     */
    public LongbowProtoData() {
    }

    @Override
    public Map<String, List<byte[]>> parse(List<Result> scanResult) {
        ArrayList<byte[]> data = new ArrayList<>();

        for (int i = 0; i < scanResult.size(); i++) {
            data.add(i, scanResult.get(i).getValue(COLUMN_FAMILY_NAME, Bytes.toBytes(Constants.LONGBOW_QUALIFIER_DEFAULT)));
        }

        HashMap<String, List<byte[]>> longbowData = new HashMap<>();
        longbowData.put(Constants.LONGBOW_PROTO_DATA_KEY, data);
        return longbowData;
    }
}
