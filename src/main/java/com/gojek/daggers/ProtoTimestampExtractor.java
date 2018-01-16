package com.gojek.daggers;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedFieldReference;
import org.apache.flink.table.expressions.StreamRecordTimestamp;
import org.apache.flink.table.sources.tsextractors.TimestampExtractor;

import java.util.ArrayList;

public class ProtoTimestampExtractor extends TimestampExtractor {

    private ArrayList<String> attributes = new ArrayList<>();

    ProtoTimestampExtractor(String rowTimeAttributeName) {
        attributes.add(rowTimeAttributeName);
    }

    @Override
    public String[] getArgumentFields() {
        return attributes.toArray(new String[0]);
    }

    @Override
    public void validateArgumentFields(TypeInformation<?>[] argumentFieldTypes) throws ValidationException {

    }

    @Override
    public Expression getExpression(ResolvedFieldReference[] fieldAccesses) {
        Expression fieldAccess = fieldAccesses[0];
        System.out.println(fieldAccess.children());

        return new StreamRecordTimestamp();
    }
}
