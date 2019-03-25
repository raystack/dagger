package com.gojek.daggers.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gojek.esb.customer.CustomerLogMessage;
import com.google.gson.Gson;
import com.google.protobuf.Descriptors;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.gojek.esb.types.GenderTypeProto.GenderType.Enum.MALE;
import static org.junit.Assert.assertNotNull;

public class RowMakerTest {

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void makeRow() {
        Descriptors.Descriptor descriptor = CustomerLogMessage.getDescriptor();
        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("customer_id", 144614);
        inputMap.put("customer_url", "https://www.abcd.com/1234");
        inputMap.put("active", "true");
        inputMap.put("sex", MALE);
//        inputMap.put("new_user", false);
        inputMap.put("created_at", "2016-01-18T08:55:26.16Z");
        Row row = RowMaker.makeRow(inputMap, descriptor);
        assertNotNull(row);
    }

}
