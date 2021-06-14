package io.odpf.dagger.functions.udfs.aggregate.accumulator;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * The accumulator for CollectArray udf.
 */
public class ArrayAccumulator implements Serializable {

    private ArrayList<Object> arrayList = new ArrayList<>();

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
    public ArrayList<Object> emit() {
        return arrayList;
    }
}
