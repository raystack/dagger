package io.odpf.dagger.core.processors.longbow.data;

import org.apache.hadoop.hbase.client.Result;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * The interface Longbow data.
 */
public interface LongbowData extends Serializable {
    /**
     * Parse the scan result.
     *
     * @param scanResult the scan result
     * @return the map
     */
    Map parse(List<Result> scanResult);
}
