package io.odpf.dagger.core.processors.longbow.columnmodifier;

import io.odpf.dagger.core.utils.Constants;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * The Longbow write column modifier.
 */
public class LongbowWriteColumnModifier implements ColumnModifier {

    @Override
    public String[] modifyColumnNames(String[] inputColumnNames) {
        ArrayList<String> outputList = new ArrayList<>(Arrays.asList(inputColumnNames));
        outputList.add(Constants.SYNCHRONIZER_BIGTABLE_TABLE_ID_KEY);
        outputList.add(Constants.SYNCHRONIZER_INPUT_CLASSNAME_KEY);
        outputList.add(Constants.SYNCHRONIZER_LONGBOW_READ_KEY);
        return outputList.toArray(new String[0]);
    }
}
