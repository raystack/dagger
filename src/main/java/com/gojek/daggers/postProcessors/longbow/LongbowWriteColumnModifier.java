package com.gojek.daggers.postProcessors.longbow;

import java.util.ArrayList;
import java.util.Arrays;

import static com.gojek.daggers.utils.Constants.*;

public class LongbowWriteColumnModifier implements ColumnNameModifier {

    @Override
    public String[] modifyColumnNames(String[] inputColumnNames) {
        ArrayList<String> outputList = new ArrayList<>(Arrays.asList(inputColumnNames));
        outputList.add(SYNCHRONIZER_BIGTABLE_TABLE_ID_KEY);
        outputList.add(SYNCHRONIZER_INPUT_CLASSNAME_KEY);
        outputList.add(SYNCHRONIZER_LONGBOWREAD_KEY);
        return outputList.toArray(new String[0]);
    }
}
