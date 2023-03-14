package com.gotocompany.dagger.functions.udfs.scalar;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import com.gotocompany.dagger.common.udfs.ScalarUdf;
import org.apache.flink.table.annotation.DataTypeHint;

import java.io.Serializable;

/**
 * The Json delete udf.
 */
public class JsonDelete extends ScalarUdf implements Serializable {

    /**
     * Deletes JSON node or value from a JSON path.
     * The result is always returned as a JSON STRING.
     *
     * @param jsonEvent the json String
     * @param jPath     the jPath
     * @return jsonString
     */
    public @DataTypeHint("STRING") String eval(String jsonEvent, String jPath) throws PathNotFoundException {
        Configuration configuration = Configuration.defaultConfiguration().setOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
        return JsonPath.using(configuration).parse(jsonEvent).delete(JsonPath.compile(jPath)).jsonString();
    }
}
