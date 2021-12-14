package io.odpf.dagger.functions.udfs.aggregate.accumulator;

import org.apache.flink.table.annotation.DataTypeHint;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The accumulator for CollectArray udf.
 */
public class ArrayAccumulator implements Serializable {

    private @DataTypeHint("RAW") List<Object> arrayList = new ArrayList<>();

    /**
     * Add object to array list.
     *
     * @param object the object
     */
    public void add(Object object) {
        arrayList.add(object);
    }

    /**
     * Emit array list.
     *
     * @return the array list
     */
    public List<Object> emit() {
        return arrayList;
    }

    public List<Object> getArrayList() {
        return arrayList;
    }

    public void setArrayList(List<Object> arrayList) {
        this.arrayList = arrayList;
    }
}
