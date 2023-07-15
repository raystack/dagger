package org.raystack.dagger.functions.udfs.scalar;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import org.raystack.dagger.common.udfs.ScalarUdf;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;

import java.io.Serializable;

/**
 * The Json update udf.
 */
public class JsonUpdate extends ScalarUdf implements Serializable {

    /**
     * Updates JSON  from a JSON path.
     * The result is always returned as a JSON STRING.
     *
     * @param jsonEvent the json String
     * @param jPath     the jPath
     * @param newValue       the new object value
     * @return jsonString
     */
    public @DataTypeHint("STRING") String eval(String jsonEvent, String jPath, @DataTypeHint(inputGroup = InputGroup.ANY) Object newValue) throws PathNotFoundException {
        Configuration configuration = Configuration.defaultConfiguration().setOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
        return JsonPath.using(configuration).parse(jsonEvent).set(JsonPath.compile(jPath), newValue).jsonString();
    }
}
