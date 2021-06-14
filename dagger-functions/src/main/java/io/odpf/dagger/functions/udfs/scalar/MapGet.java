package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * The Map get udf.
 */
public class MapGet extends ScalarUdf {
    /**
     * returns value for a corresponding key inside a map data type.
     *
     * @param inputMap the input map element
     * @param key      the key
     * @return the value
     * @author Ujjawal
     * @team Fraud
     */
    public Object eval(Row[] inputMap, Object key) {
        List<Row> rows = Arrays.asList(inputMap);
        Optional<Row> requiredRow = rows.stream().filter(row -> row.getField(0).equals(key)).findFirst();
        return requiredRow.map(row -> row.getField(1)).orElse(null);
    }
}
