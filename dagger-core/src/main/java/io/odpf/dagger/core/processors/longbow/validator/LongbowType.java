package io.odpf.dagger.core.processors.longbow.validator;

import io.odpf.dagger.core.utils.Constants;

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

    public String getKeyName() {
        return keyName;
    }

    public String[] getMandatoryFields() {
        return mandatoryFields;
    }

    public String[] getInvalidFields() {
        return invalidFields;
    }

    public String getTypeName() {
        return keyName.replace(LONGBOW_TYPE_PREFIX, "");
    }

    private static class MandatoryFields {
        private static final String[] LONGBOW_PROCESS = new String[]{Constants.LONGBOW_DATA, Constants.EVENT_TIMESTAMP, Constants.ROWTIME};
        private static final String[] LONGBOW_WRITE = new String[]{Constants.ROWTIME, Constants.EVENT_TIMESTAMP};
        private static final String[] LONGBOW_READ = new String[]{Constants.EVENT_TIMESTAMP};
    }

    private static class LongbowKey {
        private static final String LONGBOW_PROCESS = "longbow_key";
        private static final String LONGBOW_WRITE = "longbow_write_key";
        private static final String LONGBOW_READ = "longbow_read_key";
    }

    private static class InvalidFields {
        private static final String[] LONGBOW_PROCESS = new String[]{Constants.LONGBOW_PROTO_DATA};
        private static final String[] LONGBOW_WRITE = new String[]{Constants.LONGBOW_PROTO_DATA, Constants.LONGBOW_DATA, Constants.LONGBOW_LATEST, Constants.LONGBOW_EARLIEST, Constants.LONGBOW_DURATION};
        private static final String[] LONGBOW_READ = new String[]{Constants.LONGBOW_DATA, Constants.LONGBOW_PROTO_DATA};
    }
}
