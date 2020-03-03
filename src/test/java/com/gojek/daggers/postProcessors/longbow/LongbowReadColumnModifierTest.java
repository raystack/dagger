package com.gojek.daggers.postProcessors.longbow;

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