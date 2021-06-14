package io.odpf.dagger.core.processors.longbow.request;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.processors.longbow.range.LongbowRange;
import io.odpf.dagger.core.processors.longbow.storage.ScanRequest;
import io.odpf.dagger.core.utils.Constants;

import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * The factory class for scan request.
 */
public class ScanRequestFactory implements Serializable {
    private LongbowSchema longbowSchema;
    private String tableId;

    /**
     * Instantiates a new Scan request factory.
     *
     * @param longbowSchema the longbow schema
     * @param tableId       the table id
     */
    public ScanRequestFactory(LongbowSchema longbowSchema, String tableId) {
        this.longbowSchema = longbowSchema;
        this.tableId = tableId;
    }

    /**
     * Create scan request.
     *
     * @param input        the input
     * @param longbowRange the longbow range
     * @return the scan request
     */
    public ScanRequest create(Row input, LongbowRange longbowRange) {
        if (!longbowSchema.isLongbowPlus()) {
            return new TableScanRequest(longbowRange.getUpperBound(input), longbowRange.getLowerBound(input), longbowSchema, tableId);
        } else {
            return new ProtoByteScanRequest(longbowRange.getUpperBound(input), longbowRange.getLowerBound(input), parseTableName(input));
        }
    }

    private String parseTableName(Row input) {
        return (String) longbowSchema.getValue(input, Constants.SYNCHRONIZER_BIGTABLE_TABLE_ID_KEY);
    }
}
