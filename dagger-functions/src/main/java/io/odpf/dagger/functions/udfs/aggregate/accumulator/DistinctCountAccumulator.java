package io.odpf.dagger.functions.udfs.aggregate.accumulator;

import org.apache.flink.table.annotation.DataTypeHint;

import java.io.Serializable;
import java.util.HashSet;

/**
 * The accumulator for DistinctCount udf.
 */
public class DistinctCountAccumulator implements Serializable {

    private @DataTypeHint("RAW") HashSet<String> distinctItems = new HashSet<>();

    /**
     * Count size of the distinct items.
     *
     * @return the size of distinct items
     */
    public int count() {
        return distinctItems.size();
    }

    /**
     * Add item.
     *
     * @param item the item
     */
    public void add(String item) {
        distinctItems.add(item);
    }

    public HashSet<String> getDistinctItems() {
        return distinctItems;
    }

    public void setDistinctItems(HashSet<String> distinctItems) {
        this.distinctItems = distinctItems;
    }
}
