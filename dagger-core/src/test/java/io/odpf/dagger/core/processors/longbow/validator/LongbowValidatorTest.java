package io.odpf.dagger.core.processors.longbow.validator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
        try {
            longbowValidator.validateLongbow(LongbowType.LongbowWrite);
        } catch (Exception e) {

            assertEquals("Missing required field: rowtime in Longbow type : longbow_write", e.getMessage());
        }
    }

    @Test
    public void shouldThrowValidationWithInvalidField() {
        String[] columnNames = new String[]{"longbow_key", "rowtime", "longbow_data", "event_timestamp"};
        LongbowValidator longbowValidator = new LongbowValidator(columnNames);
        try {
            longbowValidator.validateLongbow(LongbowType.LongbowWrite);
        } catch (Exception e) {

            assertEquals("Invalid fields present : longbow_data in Longbow type : longbow_write", e.getMessage());
        }
    }
}
