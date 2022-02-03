package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Split udf.
 */
public class Split extends ScalarUdf {
    private static final Logger LOGGER = LoggerFactory.getLogger(Split.class.getName());

    /**
     * This UDF returns array of string that has been split from input string.
     *
     * @param input     input string that want to be split
     * @param delimiter separator or delimiter to split the input string
     * @return array of string that has been split
     * @author jesry.pandawa
     * @team DE
     */
    //Works fine dont need any type hints
    public String[] eval(String input, String delimiter) {
        if (input == null || delimiter == null) {
            LOGGER.info("Not able to split input string, either input string or delimiter is null");
            return new String[0];
        }
        if (delimiter.equals("")) {
            delimiter = " ";
        }

        return input.split(delimiter);
    }
}
