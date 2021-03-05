package com.gojek.daggers.postprocessors.longbow;

import com.gojek.daggers.postprocessors.longbow.columnmodifier.LongbowReadColumnModifier;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class LongbowReadColumnModifierTest {
    @Test
    public void shouldAddProtoColumnNames() {
        LongbowReadColumnModifier longbowReadColumnModifier = new LongbowReadColumnModifier();
        String[] inputColumnNames = {};
        String[] outputColumnNames = longbowReadColumnModifier.modifyColumnNames(inputColumnNames);
        String[] expected = {"proto_data"};
        assertArrayEquals(expected, outputColumnNames);
    }
}
