package io.odpf.dagger.core.processors.longbow.validator;

import io.odpf.dagger.core.exception.DaggerConfigurationException;
import org.junit.Test;

import static org.junit.Assert.*;

public class LongbowValidatorTest {

    @Test
    public void shouldNotThrowErrorWhenFieldsAreComplete() {
        String[] columnNames = new String[]{"longbow_key", "rowtime", "event_timestamp"};
        LongbowValidator longbowValidator = new LongbowValidator(columnNames);

        longbowValidator.validateLongbow(LongbowType.LongbowWrite);
    }

    @Test
    public void shouldThrowValidationErrorWhenMandatoryFieldIsMissing() {
        String[] columnNames = new String[]{"event_timestamp"};
        LongbowValidator longbowValidator = new LongbowValidator(columnNames);
        DaggerConfigurationException configException = assertThrows(DaggerConfigurationException.class,
                () -> longbowValidator.validateLongbow(LongbowType.LongbowWrite));
        assertEquals("Missing required field: rowtime in Longbow type : longbow_write", configException.getMessage());
    }

    @Test
    public void shouldThrowValidationWithInvalidField() {
        String[] columnNames = new String[]{"longbow_key", "rowtime", "longbow_data", "event_timestamp"};
        LongbowValidator longbowValidator = new LongbowValidator(columnNames);
        DaggerConfigurationException configException = assertThrows(DaggerConfigurationException.class,
                () -> longbowValidator.validateLongbow(LongbowType.LongbowWrite));
        assertEquals("Invalid fields present : longbow_data in Longbow type : longbow_write", configException.getMessage());

    }
}
