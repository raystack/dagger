package io.odpf.dagger.core.processors.longbow;

import io.odpf.dagger.core.processors.longbow.columnmodifier.LongbowReadColumnModifier;
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
