package com.gojek.daggers.postProcessors.longbow.storage;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.request.ProtoByteScanRequest;
import com.gojek.daggers.postProcessors.longbow.request.TableScanRequest;
import org.apache.hadoop.hbase.client.Scan;

import java.io.Serializable;

public interface ScanRequest {
    Scan get();

    default Scan setScanRange(byte[] startRow, byte[] stopRow) {
        Scan scan = new Scan();
        scan.withStartRow(startRow, true);
        scan.withStopRow(stopRow, true);
        return scan;
    }

    class ScanRequestFactory implements Serializable {
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
}
