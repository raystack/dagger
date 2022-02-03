package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.dagger.functions.exceptions.InvalidNumberOfArgumentsException;
import io.odpf.dagger.functions.udfs.aggregate.accumulator.FeatureWithTypeAccumulator;
import io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.types.Row;

import static io.odpf.dagger.functions.common.Constants.NUMBER_OF_ARGUMENTS_IN_FEATURE_ACCUMULATOR;

/**
 * The Single feature with type udf.
 */
public class SingleFeatureWithType extends ScalarUdf {
    /**
     * Converts the given list of objects to a FeatureRow type to store in feast(https://github.com/feast-dev/feast) with
     * key and values from first two args from every triplet passed in args and data type according to third element of triplet.
     * This is to be used when there is no aggregation involved and we want to convert only one event to feature row. Unlike the aggregator
     *
     * @param objects the objects
     * @return featuresWithRow the list of featureRows in form of flink Row
     * @author gaurav.singhania
     * @team DE
     */

    public @DataTypeHint("RAW") Row[] eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... objects) {
        FeatureWithTypeAccumulator featureAccumulator = new FeatureWithTypeAccumulator();
        if (objects.length % NUMBER_OF_ARGUMENTS_IN_FEATURE_ACCUMULATOR != 0) {
            throw new InvalidNumberOfArgumentsException();
        }
        for (int elementIndex = 0; elementIndex < objects.length; elementIndex += NUMBER_OF_ARGUMENTS_IN_FEATURE_ACCUMULATOR) {
            featureAccumulator.add(String.valueOf(objects[elementIndex]), objects[elementIndex + 1], ValueEnum.valueOf(String.valueOf(objects[elementIndex + 2])));
        }
        return featureAccumulator.getFeaturesAsRows();
    }

}
