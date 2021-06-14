package io.odpf.dagger.core.processors.longbow.data;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.utils.Constants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The Longbow table data.
 */
public class LongbowTableData implements LongbowData {

    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(Constants.LONGBOW_COLUMN_FAMILY_DEFAULT);
    private LongbowSchema longbowSchema;

    /**
     * Instantiates a new Longbow table data.
     *
     * @param longbowSchema the longbow schema
     */
    public LongbowTableData(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    @Override
    public Map<String, List<String>> parse(List<Result> scanResult) {
        Map<String, List<String>> longbowData = new HashMap<>();
        List<String> longbowDataColumnNames = longbowSchema.getColumnNames(c -> c.getKey().contains(Constants.LONGBOW_DATA_KEY));
        if (scanResult.isEmpty()) {
            longbowDataColumnNames.forEach(name -> longbowData.put(name, new ArrayList<>()));
        } else {
            longbowDataColumnNames.forEach(name -> longbowData.put(name, getData(scanResult, name)));
        }
        return longbowData;
    }

    private List<String> getData(List<Result> resultScan, String name) {
        return resultScan
                .stream()
                .map(result -> Bytes.toString(result.getValue(COLUMN_FAMILY_NAME, Bytes.toBytes(name))))
                .collect(Collectors.toList());
    }
}
