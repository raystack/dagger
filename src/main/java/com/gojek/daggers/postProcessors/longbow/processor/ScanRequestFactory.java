package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;

import java.io.Serializable;

import static com.gojek.daggers.utils.Constants.LONGBOW_DATA;

public class ScanRequestFactory implements Serializable {
    private LongbowSchema longbowSchema;

    public ScanRequestFactory(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    public ScanRequest create(byte[] startRow, byte[] stopRow) {
        if (longbowSchema.hasLongbowData()) {
            return new TableScanRequest(startRow, stopRow, longbowSchema);
        } else return new ProtoByteScanRequest(startRow, stopRow);
    }
}
