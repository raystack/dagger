package io.odpf.dagger.functions.udfs.aggregate.accumulator;

import java.io.Serializable;
import java.util.ArrayList;

public class ArrayAccumulator implements Serializable {

    private ArrayList<Object> arrayList = new ArrayList<>();

    public void add(Object e) {
        arrayList.add(e);
    }

    public ArrayList<Object> emit() {
        return arrayList;
    }
}
