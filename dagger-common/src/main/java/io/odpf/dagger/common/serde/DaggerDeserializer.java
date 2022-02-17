package io.odpf.dagger.common.serde;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

public interface DaggerDeserializer<T> extends Serializable, ResultTypeQueryable<T> {

}