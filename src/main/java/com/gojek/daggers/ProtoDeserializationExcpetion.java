package com.gojek.daggers;

public class ProtoDeserializationExcpetion extends RuntimeException {
  public ProtoDeserializationExcpetion(String message) {
    super(message);
  }

  public ProtoDeserializationExcpetion(String message, Exception innerException) {
    super(message, innerException);
  }

  public ProtoDeserializationExcpetion(Exception innerException) {
    super(innerException);
  }
}
