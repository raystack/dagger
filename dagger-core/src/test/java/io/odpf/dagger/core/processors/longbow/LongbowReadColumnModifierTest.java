package io.odpf.dagger.core.processors.longbow;

import io.odpf.dagger.core.processors.longbow.columnmodifier.LongbowReadColumnModifier;
import org.junit.Test;

import static org.junit.Assert.*;

public class LongbowReadColumnModifierTest {
    @Test
    public void shouldReturnProtoDataWhenEmptyInputColumnNames() {
        LongbowReadColumnModifier longbowReadColumnModifier = new LongbowReadColumnModifier();
        String[] inputColumnNames = {};
        String[] outputColumnNames = longbowReadColumnModifier.modifyColumnNames(inputColumnNames);
        String[] expected = {"proto_data"};
        assertArrayEquals(expected, outputColumnNames);
    }

    @Test
    public void shouldAddProtoColumnNames() {
        LongbowReadColumnModifier longbowReadColumnModifier = new LongbowReadColumnModifier();
        String[] inputColumnNames = {"hello", "world"};
        String[] outputColumnNames = longbowReadColumnModifier.modifyColumnNames(inputColumnNames);
        String[] expected = {"hello", "world", "proto_data"};
        assertArrayEquals(expected, outputColumnNames);
    }
}
