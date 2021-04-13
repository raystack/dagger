package io.odpf.dagger.processors.longbow.columnmodifier;

import io.odpf.dagger.utils.Constants;

import java.util.ArrayList;
import java.util.Arrays;

public class LongbowWriteColumnModifier implements ColumnModifier {

    @Override
    public String[] modifyColumnNames(String[] inputColumnNames) {
        ArrayList<String> outputList = new ArrayList<>(Arrays.asList(inputColumnNames));
        outputList.add(Constants.SYNCHRONIZER_BIGTABLE_TABLE_ID_KEY);
        outputList.add(Constants.SYNCHRONIZER_INPUT_CLASSNAME_KEY);
        outputList.add(Constants.SYNCHRONIZER_LONGBOWREAD_KEY);
        return outputList.toArray(new String[0]);
    }
}
