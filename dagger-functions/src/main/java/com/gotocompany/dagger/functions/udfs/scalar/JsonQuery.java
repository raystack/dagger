package com.gotocompany.dagger.functions.udfs.scalar;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.gotocompany.dagger.common.udfs.ScalarUdf;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.annotation.DataTypeHint;

import java.io.Serializable;
import java.util.Objects;

/**
 * The Json query udf.
 */
public class JsonQuery extends ScalarUdf implements Serializable {

    /**
     * Extracts JSON values from a JSON string.
     * The result is always returned as a JSON STRING.
     *
     * @param jsonEvent the json String
     * @param jPath     the jPath
     * @return jsonString
     */
    public @DataTypeHint("STRING") String eval(@DataTypeHint("STRING") String jsonEvent, @DataTypeHint("STRING") String jPath) throws JsonProcessingException {
        Configuration configuration = Configuration.defaultConfiguration().setOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
        Object jChildObject = JsonPath.using(configuration).parse(jsonEvent).read(JsonPath.compile(jPath));
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.USE_DEFAULTS);
        return Objects.isNull(jChildObject) ? null : mapper.writeValueAsString(jChildObject);
    }
}
