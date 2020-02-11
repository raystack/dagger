package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.storage.PutRequest;
import com.gojek.daggers.sink.ProtoSerializer;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static com.gojek.daggers.utils.Constants.LONGBOW_DATA;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class PutRequestFactoryTest {

    @Mock
    private LongbowSchema longbowSchema;

    @Mock
    private ProtoSerializer protoSerializer;

    @Mock
    private Row row;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldCreateTablePutRequestWhenLongbowSchemaHasLongbowData() {
        when(longbowSchema.contains(LONGBOW_DATA)).thenReturn(true);
        PutRequestFactory putRequestFactory = new PutRequestFactory(longbowSchema, protoSerializer);
        PutRequest putRequest = putRequestFactory.create(row);
        Assert.assertEquals(putRequest.getClass(), TablePutRequest.class);
    }

    @Test
    public void shouldCreateProtoPutRequestWhenLongbowSchemaHasLongbowData() {
        when(longbowSchema.contains(LONGBOW_DATA)).thenReturn(false);
        PutRequestFactory putRequestFactory = new PutRequestFactory(longbowSchema, protoSerializer);
        PutRequest putRequest = putRequestFactory.create(row);
        Assert.assertEquals(ProtoBytePutRequest.class, putRequest.getClass());
    }
}