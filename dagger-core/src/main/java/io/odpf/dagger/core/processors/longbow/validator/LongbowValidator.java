package io.odpf.dagger.core.processors.longbow.validator;

import io.odpf.dagger.core.exception.DaggerConfigurationException;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * The Longbow validator.
 */
public class LongbowValidator {
    private String[] columnNames;

    /**
     * Instantiates a new Longbow validator.
     *
     * @param columnNames the column names
     */
    public LongbowValidator(String[] columnNames) {
        this.columnNames = columnNames;
    }

    /**
     * Validate longbow.
     *
     * @param longbowType the longbow type
     */
    public void validateLongbow(LongbowType longbowType) {
        String missingFields = Arrays
                .stream(longbowType.getMandatoryFields())
                .filter(field -> Arrays.stream(columnNames)
                        .noneMatch(columnName -> columnName.contains(field)))
                .collect(Collectors.joining(","));

        String wrongFields = Arrays.stream(longbowType.getInvalidFields())
                .filter(field -> Arrays.stream(columnNames)
                        .anyMatch(columnName -> columnName.contains(field)))
                .collect(Collectors.joining(","));

        if (StringUtils.isNotEmpty(missingFields)) {
            throw new DaggerConfigurationException("Missing required field: " + missingFields + " in Longbow type : " + longbowType.getTypeName());
        }
        if (StringUtils.isNotEmpty(wrongFields)) {
            throw new DaggerConfigurationException("Invalid fields present : " + wrongFields + " in Longbow type : " + longbowType.getTypeName());
        }
    }
}
