package com.gojek.daggers.sink;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

public class InfluxDBFactoryWrapper {

    public InfluxDB connect(String url, String username, String password){
        return InfluxDBFactory.connect(url, username, password);
    }
}
