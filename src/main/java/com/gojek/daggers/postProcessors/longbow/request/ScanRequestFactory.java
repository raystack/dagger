package com.gojek.daggers.postProcessors.longbow.request;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.storage.ScanRequest;

import java.io.Serializable;

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
