package io.odpf.dagger.functions.udfs.scalar;

import com.google.protobuf.ByteString;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ByteToStringTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldConvertProtoBytesToString() {
        ByteToString byteToString = new ByteToString();
        ByteString byteString = ByteString.copyFrom("testString".getBytes(StandardCharsets.UTF_8));
        assertEquals("testString", byteToString.eval(byteString));
    }

    @Test
    public void shouldConvertProtoBytesToStringForEmptyBytes() {
        ByteToString byteToString = new ByteToString();
        ByteString byteString = ByteString.copyFrom("".getBytes(StandardCharsets.UTF_8));
        assertEquals("", byteToString.eval(byteString));
    }

    @Test
    public void shouldThrowNullPointerExceptionIfPassedForNullField() {
        thrown.expect(NullPointerException.class);
        ByteToString byteToString = new ByteToString();
        assertEquals("", byteToString.eval(null));
    }

    @Test
    public void inputTypeStrategyTest() {
        InputTypeStrategy inputTypeStrategy = new ByteToString().getTypeInference(null).getInputTypeStrategy();
        CallContext mockContext = mock(CallContext.class);
        DataType bigint = DataTypes.BIGINT();
        when(mockContext.getArgumentDataTypes()).thenReturn(Arrays.asList(bigint));
        Optional<List<DataType>> dataTypes = inputTypeStrategy.inferInputTypes(mockContext, false);
        assertEquals(Optional.of(Arrays.asList(bigint)), dataTypes);
        assertEquals(ConstantArgumentCount.of(1), inputTypeStrategy.getArgumentCount());
    }

    @Test
    public void outputTypeStrategy() {
        TypeStrategy byteStringOutputStrategy = new ByteToString().getTypeInference(null).getOutputTypeStrategy();
        CallContext mockContext = mock(CallContext.class);
        Optional<DataType> dataType = byteStringOutputStrategy.inferType(mockContext);
        assertEquals(Optional.of(DataTypes.STRING()), dataType);
        verifyZeroInteractions(mockContext);
    }
}
