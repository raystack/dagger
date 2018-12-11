package com.gojek.daggers.sink.influx;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import java.io.Serializable;

public class InfluxDBFactoryWrapper implements Serializable {

  public InfluxDB connect(String url, String username, String password) {
    return InfluxDBFactory.connect(url, username, password);
  }
}
