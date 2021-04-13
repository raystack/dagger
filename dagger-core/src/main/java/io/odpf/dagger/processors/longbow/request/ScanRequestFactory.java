package io.odpf.dagger.processors.longbow.request;

import io.odpf.dagger.processors.longbow.LongbowSchema;
import io.odpf.dagger.processors.longbow.range.LongbowRange;
import io.odpf.dagger.processors.longbow.storage.ScanRequest;
import io.odpf.dagger.utils.Constants;

import org.apache.flink.types.Row;

import java.io.Serializable;

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
        return (String) longbowSchema.getValue(input, Constants.SYNCHRONIZER_BIGTABLE_TABLE_ID_KEY);
    }
}
