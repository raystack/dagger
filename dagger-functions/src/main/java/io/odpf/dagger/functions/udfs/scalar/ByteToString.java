package io.odpf.dagger.functions.udfs.scalar;

import com.google.protobuf.ByteString;
import io.odpf.dagger.common.udfs.ScalarUdf;

public class ByteToString extends ScalarUdf {
    public String eval(ByteString byteField) {
        return byteField.toStringUtf8();
    }
}
