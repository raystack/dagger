package io.odpf.dagger.core.sink.influx;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import java.io.Serializable;

/**
 * The Influx db factory wrapper.
 */
public class InfluxDBFactoryWrapper implements Serializable {

    /**
     * Connect influx db.
     *
     * @param url      the url
     * @param username the username
     * @param password the password
     * @return the influx db
     */
    public InfluxDB connect(String url, String username, String password) {
        return InfluxDBFactory.connect(url, username, password);
    }
}
