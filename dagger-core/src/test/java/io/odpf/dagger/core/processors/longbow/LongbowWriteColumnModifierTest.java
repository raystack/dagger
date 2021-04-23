package io.odpf.dagger.core.processors.longbow;

import io.odpf.dagger.core.processors.longbow.columnmodifier.LongbowWriteColumnModifier;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class LongbowWriteColumnModifierTest {
    @Test
    public void shouldAddSynchronizerColumnNames() {
        LongbowWriteColumnModifier longbowWriteColumnModifier = new LongbowWriteColumnModifier();
        String[] inputColumnNames = {};
        String[] outputColumnNames = longbowWriteColumnModifier.modifyColumnNames(inputColumnNames);
        String[] expected = {"bigtable_table_id", "input_class_name", "longbow_read_key"};
        assertArrayEquals(expected, outputColumnNames);
    }
}
