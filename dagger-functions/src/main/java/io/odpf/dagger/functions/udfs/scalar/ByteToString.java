package io.odpf.dagger.functions.udfs.scalar;

import com.google.protobuf.ByteString;
import io.odpf.dagger.common.udfs.ScalarUdf;

public class ByteToString extends ScalarUdf {
    /**
     * Given a ByteString, this UDF converts to String.
     *
     * @param byteField the field with byte[] in proto
     * @return string value of byteField
     */
    public String eval(ByteString byteField) {
        return byteField.toStringUtf8();
    }
}
