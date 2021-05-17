package io.odpf.dagger.functions.udfs.aggregate.accumulator;

import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;

public class DistinctCountAccumulatorTest {

    @Test
    public void shouldGiveDistinctCount() {
        DistinctCountAccumulator accumulator = new DistinctCountAccumulator();
        accumulator.add("First");
        accumulator.add("Second");
        accumulator.add("First");
        accumulator.add("Third");
        accumulator.add("Second");

        assertEquals(accumulator.count(), 3);
    }

    @Test
    public void shouldBeSerializable() throws IOException, ClassNotFoundException {
        DistinctCountAccumulator accumulator = new DistinctCountAccumulator();
        accumulator.add("First");
        accumulator.add("Second");
        accumulator.add("First");

        ByteArrayOutputStream serializedAccumulatorStream = new ByteArrayOutputStream();
        new ObjectOutputStream(serializedAccumulatorStream).writeObject(accumulator);

        ObjectInputStream deserializedAccStream = new ObjectInputStream(new ByteArrayInputStream(serializedAccumulatorStream.toByteArray()));

        DistinctCountAccumulator deserializedAccumulator = (DistinctCountAccumulator) deserializedAccStream.readObject();

        assertEquals(deserializedAccumulator.count(), accumulator.count());
    }
}
