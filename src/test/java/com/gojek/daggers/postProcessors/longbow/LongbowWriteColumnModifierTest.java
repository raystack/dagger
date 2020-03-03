package com.gojek.daggers.postProcessors.longbow;

import org.junit.Test;

import static org.junit.Assert.*;

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