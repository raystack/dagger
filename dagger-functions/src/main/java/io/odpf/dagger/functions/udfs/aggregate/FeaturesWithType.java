package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.common.udfs.AggregateUdf;
import io.odpf.dagger.functions.exceptions.InvalidNumberOfArgumentsException;
import io.odpf.dagger.functions.udfs.aggregate.accumulator.FeatureWithTypeAccumulator;
import io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.types.Row;

import static io.odpf.dagger.functions.common.Constants.NUMBER_OF_ARGUMENTS_IN_FEATURE_ACCUMULATOR;

/**
 * User-defined aggregate function to get Features with type.
 */
@FunctionHint(output = @DataTypeHint("RAW"))
public class FeaturesWithType extends AggregateUdf<Row[], FeatureWithTypeAccumulator> {

    @Override
    public FeatureWithTypeAccumulator createAccumulator() {
        return new FeatureWithTypeAccumulator();
    }

    @Override
    public Row[] getValue(FeatureWithTypeAccumulator featureAccumulator) {
        return featureAccumulator.getFeaturesAsRows();
    }

    /**
     * Converts the given list of objects to a FeatureRow type to store in feast(https://github.com/feast-dev/feast)
     * with key and values from first two args from every triplet passed in args and data type according to third element of triplet.
     *
     * @param featureAccumulator the feature accumulator
     * @param objects            the objects
     * @return featuresWithRow the list of featureRows in form of flink Row
     * @author grace.christina
     * @team DS
     */
    public void accumulate(FeatureWithTypeAccumulator featureAccumulator, @DataTypeHint(inputGroup = InputGroup.ANY) Object... objects) {
        validate(objects);
        for (int elementIndex = 0; elementIndex < objects.length; elementIndex += NUMBER_OF_ARGUMENTS_IN_FEATURE_ACCUMULATOR) {
            featureAccumulator.add(String.valueOf(objects[elementIndex]), objects[elementIndex + 1], ValueEnum.valueOf(String.valueOf(objects[elementIndex + 2])));
        }
    }

    /**
     * Retract.
     *
     * @param featureAccumulator the feature accumulator
     * @param objects            the objects
     */
    public void retract(FeatureWithTypeAccumulator featureAccumulator, Object... objects) {
        validate(objects);
        for (int elementIndex = 0; elementIndex < objects.length; elementIndex += NUMBER_OF_ARGUMENTS_IN_FEATURE_ACCUMULATOR) {
            featureAccumulator.remove(String.valueOf(objects[elementIndex]), objects[elementIndex + 1], ValueEnum.valueOf(String.valueOf(objects[elementIndex + 2])));
        }
    }

    public void merge(FeatureWithTypeAccumulator featureWithTypeAccumulator, Iterable<FeatureWithTypeAccumulator> it) {
        for (FeatureWithTypeAccumulator accumulatorInstance : it) {
            accumulatorInstance.getFeatures().forEach((s, tuple3) -> featureWithTypeAccumulator.add(tuple3.f0, tuple3.f1, tuple3.f2));
        }
    }

    private void validate(Object[] objects) {
        if (objects.length % NUMBER_OF_ARGUMENTS_IN_FEATURE_ACCUMULATOR != 0) {
            throw new InvalidNumberOfArgumentsException();
        }
    }


}
