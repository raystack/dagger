package io.odpf.dagger.processors.longbow.columnmodifier;

import io.odpf.dagger.utils.Constants;

import java.util.ArrayList;
import java.util.Arrays;

public class LongbowReadColumnModifier implements ColumnModifier {

    @Override
    public String[] modifyColumnNames(String[] inputColumnNames) {
        ArrayList<String> inputColumnList = new ArrayList<>(Arrays.asList(inputColumnNames));
        inputColumnList.add(inputColumnList.size(), Constants.LONGBOW_PROTO_DATA);
        return inputColumnList.toArray(new String[0]);
    }
}
