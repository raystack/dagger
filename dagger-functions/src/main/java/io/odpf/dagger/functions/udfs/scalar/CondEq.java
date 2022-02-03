package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;

import com.google.protobuf.DynamicMessage;
import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.dagger.functions.udfs.scalar.longbow.MessageParser;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * The CondEq udf.
 */
public class CondEq extends ScalarUdf {

    /**
     * This is one of the UDFs related to LongbowPlus and has to be used with SelectFields and Filters UDFs.
     * specify an equality condition with a fieldName and a value.
     *
     * @param fieldName  the field name
     * @param comparison the comparison value
     * @return the predicate condition
     * @author : Rasyid
     * @team : DE
     */
    @DataTypeHint(value = "RAW", bridgedTo = Predicate.class)
    public Predicate<DynamicMessage> eval(String fieldName, @DataTypeHint(inputGroup = InputGroup.ANY) Object comparison) {
        return new Predicate<DynamicMessage>() {
            @Override
            public boolean test(DynamicMessage message) {
                MessageParser messageParser = new MessageParser();
                List<String> fieldNames = Arrays.asList(fieldName.split("\\."));
                return comparison.equals(messageParser.read(message, fieldNames));
            }
        };
    }
}
