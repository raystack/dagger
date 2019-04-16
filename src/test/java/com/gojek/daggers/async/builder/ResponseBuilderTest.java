package com.gojek.daggers.async.builder;

import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.*;

public class ResponseBuilderTest {

    @Test
    public void shouldCreateResponseWithTheGivenRowAtGivenIndex() {
        ResponseBuilder responseBuilder = new ResponseBuilder(1);
        Row row = new Row(1);
        row.setField(0, "test");
        ResponseBuilder actualBuilder = responseBuilder.with(0, row);
        assertEquals(((Row)actualBuilder.build().getField(0)).getField(0), "test");
    }

    @Test
    public void shouldReturnRow() {
        Row row = new Row(1);
        ResponseBuilder responseBuilder = new ResponseBuilder(row);
        assertEquals(responseBuilder.build(), row);
    }
}