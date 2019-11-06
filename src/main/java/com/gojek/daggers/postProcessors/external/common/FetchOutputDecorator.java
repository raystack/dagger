package com.gojek.daggers.postProcessors.external.common;

import org.apache.flink.types.Row;

public class FetchOutputDecorator implements MapDecorator {


    @Override
    public Boolean canDecorate() {
        return false;
    }

    @Override
    public Row map(Row input) {
        RowManager rowManager = new RowManager(input);
        return rowManager.getOutputData();
    }


}
