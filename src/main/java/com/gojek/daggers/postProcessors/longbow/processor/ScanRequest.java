package com.gojek.daggers.postProcessors.longbow.processor;

import org.apache.hadoop.hbase.client.Scan;

public interface ScanRequest {
    public Scan get();

    default Scan setScanRange(byte[] startRow, byte[] stopRow) {
        Scan scan = new Scan();
        scan.withStartRow(startRow, true);
        scan.withStopRow(stopRow, true);
        return scan;
    }
}
