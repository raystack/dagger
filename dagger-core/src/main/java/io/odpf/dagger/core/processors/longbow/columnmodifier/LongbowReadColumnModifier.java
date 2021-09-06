package io.odpf.dagger.core.processors.longbow.columnmodifier;

import io.odpf.dagger.core.utils.Constants;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * The Longbow read column modifier.
 */
public class LongbowReadColumnModifier implements ColumnModifier {

    @Override
    public String[] modifyColumnNames(String[] inputColumnNames) {
        ArrayList<String> inputColumnList = new ArrayList<>(Arrays.asList(inputColumnNames));
        inputColumnList.add(inputColumnList.size(), Constants.LONGBOW_PROTO_DATA_KEY);

        return inputColumnList.toArray(new String[0]);
    }
}
