package com.gojek.daggers.postprocessors.longbow.validator;

import static com.gojek.daggers.utils.Constants.*;

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
        private static final String[] LONGBOW_PROCESS = new String[]{LONGBOW_DATA, EVENT_TIMESTAMP, ROWTIME};
        private static final String[] LONGBOW_WRITE = new String[]{ROWTIME, EVENT_TIMESTAMP};
        private static final String[] LONGBOW_READ = new String[]{EVENT_TIMESTAMP};
    }

    private static class LongbowKey {
        private static final String LONGBOW_PROCESS = "longbow_key";
        private static final String LONGBOW_WRITE = "longbow_write_key";
        private static final String LONGBOW_READ = "longbow_read_key";
    }

    private static class InvalidFields {
        private static final String[] LONGBOW_PROCESS = new String[]{LONGBOW_PROTO_DATA};
        private static final String[] LONGBOW_WRITE = new String[]{LONGBOW_PROTO_DATA, LONGBOW_DATA, LONGBOW_LATEST, LONGBOW_EARLIEST, LONGBOW_DURATION};
        private static final String[] LONGBOW_READ = new String[]{LONGBOW_DATA, LONGBOW_PROTO_DATA};
    }
}
