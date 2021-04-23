package io.odpf.dagger.functions.udfs.aggregate.accumulator.distinctcount;

import java.io.Serializable;
import java.util.HashSet;

public class DistinctCountAccumulator implements Serializable {
    private HashSet<String> distinctItems = new HashSet<>();

    public int count() {
        return distinctItems.size();
    }

    public void add(String item) {
        distinctItems.add(item);
    }
}
