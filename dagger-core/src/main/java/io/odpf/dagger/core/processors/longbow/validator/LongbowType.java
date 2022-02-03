package io.odpf.dagger.core.processors.longbow.validator;

import io.odpf.dagger.core.utils.Constants;

import static io.odpf.dagger.common.core.Constants.ROWTIME;

/**
 * The enum Longbow type.
 */
public enum LongbowType {
    LongbowRead(LongbowKey.LONGBOW_READ, MandatoryFields.LONGBOW_READ, InvalidFields.LONGBOW_READ),
    LongbowWrite(LongbowKey.LONGBOW_WRITE, MandatoryFields.LONGBOW_WRITE, InvalidFields.LONGBOW_WRITE),
    LongbowProcess(LongbowKey.LONGBOW_PROCESS, MandatoryFields.LONGBOW_PROCESS, InvalidFields.LONGBOW_PROCESS);

    private static final String LONGBOW_TYPE_PREFIX = "_key";
    private String keyName;
    private String[] mandatoryFields;
    private String[] invalidFields;

    LongbowType(String keyName, String[] mandatoryFields, String[] invalidFields) {
        this.keyName = keyName;
        this.mandatoryFields = mandatoryFields;
        this.invalidFields = invalidFields;
    }

    /**
     * Gets key name.
     *
     * @return the key name
     */
    public String getKeyName() {
        return keyName;
    }

    /**
     * Get mandatory fields.
     *
     * @return the mandatory fields
     */
    public String[] getMandatoryFields() {
        return mandatoryFields;
    }

    /**
     * Get invalid fields.
     *
     * @return the array of invalid fields
     */
    public String[] getInvalidFields() {
        return invalidFields;
    }

    /**
     * Gets type name.
     *
     * @return the type name
     */
    public String getTypeName() {
        return keyName.replace(LONGBOW_TYPE_PREFIX, "");
    }

    private static class MandatoryFields {
        private static final String[] LONGBOW_PROCESS = new String[]{Constants.LONGBOW_DATA_KEY, Constants.EVENT_TIMESTAMP, ROWTIME};
        private static final String[] LONGBOW_WRITE = new String[]{ROWTIME, Constants.EVENT_TIMESTAMP};
        private static final String[] LONGBOW_READ = new String[]{Constants.EVENT_TIMESTAMP};
    }

    private static class LongbowKey {
        private static final String LONGBOW_PROCESS = "longbow_key";
        private static final String LONGBOW_WRITE = "longbow_write_key";
        private static final String LONGBOW_READ = "longbow_read_key";
    }

    private static class InvalidFields {
        private static final String[] LONGBOW_PROCESS = new String[]{Constants.LONGBOW_PROTO_DATA_KEY};
        private static final String[] LONGBOW_WRITE = new String[]{Constants.LONGBOW_PROTO_DATA_KEY, Constants.LONGBOW_DATA_KEY, Constants.LONGBOW_LATEST_KEY, Constants.LONGBOW_EARLIEST_KEY, Constants.LONGBOW_DURATION_KEY};
        private static final String[] LONGBOW_READ = new String[]{Constants.LONGBOW_DATA_KEY, Constants.LONGBOW_PROTO_DATA_KEY};
    }
}
