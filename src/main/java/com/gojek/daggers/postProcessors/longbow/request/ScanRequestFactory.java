package com.gojek.daggers.postProcessors.longbow.request;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.range.LongbowRange;
import com.gojek.daggers.postProcessors.longbow.storage.ScanRequest;
import org.apache.flink.types.Row;

import java.io.Serializable;

import static com.gojek.daggers.utils.Constants.SYNCHRONIZER_BIGTABLE_TABLE_ID_KEY;

public class ScanRequestFactory implements Serializable {
    private LongbowSchema longbowSchema;
    private String tableId;

    public ScanRequestFactory(LongbowSchema longbowSchema, String tableId) {
        this.longbowSchema = longbowSchema;
        this.tableId = tableId;
    }

    public ScanRequest create(Row input, LongbowRange longbowRange) {
        if (!longbowSchema.isLongbowPlus()) {
            return new TableScanRequest(longbowRange.getUpperBound(input), longbowRange.getLowerBound(input), longbowSchema, tableId);
        } else
            return new ProtoByteScanRequest(longbowRange.getUpperBound(input), longbowRange.getLowerBound(input), parseTableName(input));
    }

    private String parseTableName(Row input) {
        //TODO: add proper exception here
        return (String) longbowSchema.getValue(input, SYNCHRONIZER_BIGTABLE_TABLE_ID_KEY);
    }
}
