package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.storage.PutRequest;

import org.apache.flink.types.Row;

public class PutRequestFactory {

  private LongbowSchema longbowSchema;

  public PutRequestFactory(LongbowSchema longbowSchema) {
    this.longbowSchema = longbowSchema;
  }

  public PutRequest create(Row input) {
    return new TablePutRequest(longbowSchema, input);
  }

}
