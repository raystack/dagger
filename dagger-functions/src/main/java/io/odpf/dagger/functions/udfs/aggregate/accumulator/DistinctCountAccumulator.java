package io.odpf.dagger.functions.udfs.aggregate.accumulator;

import java.io.Serializable;
import java.util.HashSet;

/**
 * The accumulator for DistinctCount udf.
 */
public class DistinctCountAccumulator implements Serializable {
    private HashSet<String> distinctItems = new HashSet<>();

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
}
