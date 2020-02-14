package com.gojek.daggers.postProcessors.longbow.validator;

import static com.gojek.daggers.utils.Constants.*;

public enum LongbowType {
    LongbowRead("longbow_read", MandatoryFields.LONGBOW_READ, InvalidFields.LONGBOW_READ),
    LongbowWrite("longbow_write", MandatoryFields.LONGBOW_WRITE, InvalidFields.LONGBOW_WRITE),
    LongbowProcess("longbow", MandatoryFields.LONGBOW_PROCESS, InvalidFields.LONGBOW_PROCESS);

    private String typeValue;
    private String[] mandatoryFields;
    private String[] invalidFields;

    LongbowType(String typeValue, String[] mandatoryFields, String[] invalidFields) {
        this.typeValue = typeValue;
        this.mandatoryFields = mandatoryFields;
        this.invalidFields = invalidFields;
    }

    public String getTypeValue() {
        return typeValue;
    }

    public String[] getMandatoryFields() {
        return mandatoryFields;
    }

    public String[] getInvalidFields() {
        return invalidFields;
    }

    private static class MandatoryFields {
        private static final String[] LONGBOW_PROCESS = new String[]{LONGBOW_KEY, LONGBOW_DATA, EVENT_TIMESTAMP, ROWTIME};
        private static final String[] LONGBOW_WRITE = new String[]{LONGBOW_KEY, ROWTIME};
        private static final String[] LONGBOW_READ = new String[]{LONGBOW_KEY, EVENT_TIMESTAMP, LONGBOW_PROTO_DATA};
    }

    private static class InvalidFields {
        private static final String[] LONGBOW_PROCESS = new String[]{LONGBOW_PROTO_DATA};
        private static final String[] LONGBOW_WRITE = new String[]{LONGBOW_PROTO_DATA, LONGBOW_DATA, LONGBOW_LATEST, LONGBOW_EARLIEST, LONGBOW_DURATION};
        private static final String[] LONGBOW_READ = new String[]{LONGBOW_PROTO_DATA, LONGBOW_DATA};
    }
}
