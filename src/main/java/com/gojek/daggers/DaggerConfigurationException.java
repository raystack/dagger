package com.gojek.daggers;

public class DaggerConfigurationException extends RuntimeException {

  public DaggerConfigurationException(String message) {
    super(message);
  }

  public DaggerConfigurationException(String message, Exception innerException) {
    super(message, innerException);
  }
}
