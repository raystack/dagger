package com.gojek.daggers;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.esb.aggregate.demand.AggregatedDemandKey;
import com.gojek.esb.aggregate.demand.AggregatedDemandMessage;
import com.gojek.esb.aggregate.supply.AggregatedSupplyKey;
import com.gojek.esb.aggregate.supply.AggregatedSupplyMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;

import static com.gojek.esb.types.ServiceTypeProto.ServiceType.Enum.GO_RIDE;
import static com.gojek.esb.types.VehicleTypeProto.VehicleType.Enum.BIKE;

public class ProtoSerializerTest {

  @Test
  public void shouldSerializeKeyForDemandProto() throws InvalidProtocolBufferException {
    String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "service_type"};
    String protoClassName = "com.gojek.esb.aggregate.demand.AggregatedDemand";
    ProtoSerializer protoSerializer = new ProtoSerializer(protoClassName, columnNames, StencilClientFactory.getClient());
    long seconds = System.currentTimeMillis()  / 1000;

    Row element = new Row(5);
    Timestamp timestamp = new java.sql.Timestamp(seconds * 1000);
    com.google.protobuf.Timestamp expectedTimestamp = com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(seconds)
        .setNanos(0)
        .build();

    element.setField(0, timestamp);
    element.setField(1, timestamp);
    element.setField(2, 13);
    element.setField(3, 3322909458387959808L);
    element.setField(4, GO_RIDE);

    byte[] serializeKey = protoSerializer.serializeKey(element);

    AggregatedDemandKey actualKey = AggregatedDemandKey.parseFrom(serializeKey);

    Assert.assertEquals(expectedTimestamp, actualKey.getWindowStartTime());
    Assert.assertEquals(expectedTimestamp, actualKey.getWindowEndTime());
    Assert.assertEquals(13, actualKey.getS2IdLevel());
    Assert.assertEquals(3322909458387959808L, actualKey.getS2Id());
    Assert.assertEquals("GO_RIDE", actualKey.getServiceType().toString());
  }

  @Test
  public void shouldSerializeValueForDemandProto() throws InvalidProtocolBufferException {
    String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "service_type", "unique_customers"};
    String protoClassNamePrefix = "com.gojek.esb.aggregate.demand.AggregatedDemand";
    ProtoSerializer protoSerializer = new ProtoSerializer(protoClassNamePrefix, columnNames, StencilClientFactory.getClient());
    long seconds = System.currentTimeMillis()  / 1000;

    Row element = new Row(6);
    Timestamp timestamp = new java.sql.Timestamp(seconds * 1000);
    com.google.protobuf.Timestamp expectedTimestamp = com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(seconds)
        .setNanos(0)
        .build();

    element.setField(0, timestamp);
    element.setField(1, timestamp);
    element.setField(2, 13);
    element.setField(3, 3322909458387959808L);
    element.setField(4, GO_RIDE);
    element.setField(5, 2L);

    byte[] serializeValue = protoSerializer.serializeValue(element);

    AggregatedDemandMessage actualValue = AggregatedDemandMessage.parseFrom(serializeValue);

    Assert.assertEquals(expectedTimestamp, actualValue.getWindowStartTime());
    Assert.assertEquals(expectedTimestamp, actualValue.getWindowEndTime());
    Assert.assertEquals(13, actualValue.getS2IdLevel());
    Assert.assertEquals(3322909458387959808L, actualValue.getS2Id());
    Assert.assertEquals("GO_RIDE", actualValue.getServiceType().toString());
    Assert.assertEquals(2L, actualValue.getUniqueCustomers());
  }

  @Test
  public void shouldSerializeKeyForSupplyProto() throws InvalidProtocolBufferException {
    String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "vehicle_type"};
    String protoClassName = "com.gojek.esb.aggregate.supply.AggregatedSupply";
    ProtoSerializer protoSerializer = new ProtoSerializer(protoClassName, columnNames, StencilClientFactory.getClient());
    long startTimeInSeconds = System.currentTimeMillis()   / 1000;
    long endTimeInSeconds = (System.currentTimeMillis()  + 10000) / 1000;

    Row element = new Row(5);
    Timestamp startTimestamp = new java.sql.Timestamp(startTimeInSeconds * 1000);
    Timestamp endTimestamp = new java.sql.Timestamp(endTimeInSeconds * 1000);
    com.google.protobuf.Timestamp expectedStartTimestamp = com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(startTimeInSeconds)
        .setNanos(0)
        .build();
    com.google.protobuf.Timestamp expectedEndTimestamp = com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(endTimeInSeconds)
        .setNanos(0)
        .build();

    element.setField(0, startTimestamp);
    element.setField(1, endTimestamp);
    element.setField(2, 13);
    element.setField(3, 3322909458387959808L);
    element.setField(4, BIKE);

    byte[] serializeKey = protoSerializer.serializeKey(element);


    AggregatedSupplyKey actualKey = AggregatedSupplyKey.parseFrom(serializeKey);

    Assert.assertEquals(expectedStartTimestamp, actualKey.getWindowStartTime());
    Assert.assertEquals(expectedEndTimestamp, actualKey.getWindowEndTime());
    Assert.assertEquals(13, actualKey.getS2IdLevel());
    Assert.assertEquals(3322909458387959808L, actualKey.getS2Id());
    Assert.assertEquals("BIKE", actualKey.getVehicleType().toString());
  }

  @Test
  public void shouldSerializeValueForSupplyProto() throws InvalidProtocolBufferException {
    String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "vehicle_type", "unique_drivers"};
    String protoClassNamePrefix = "com.gojek.esb.aggregate.supply.AggregatedSupply";
    ProtoSerializer protoSerializer = new ProtoSerializer(protoClassNamePrefix, columnNames, StencilClientFactory.getClient());
    long startTimeInSeconds = System.currentTimeMillis()   / 1000;
    long endTimeInSeconds = (System.currentTimeMillis()  + 10000) / 1000;

    Row element = new Row(6);
    Timestamp startTimestamp = new java.sql.Timestamp(startTimeInSeconds * 1000);
    Timestamp endTimestamp = new java.sql.Timestamp(endTimeInSeconds * 1000);
    com.google.protobuf.Timestamp expectedStartTimestamp = com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(startTimeInSeconds)
        .setNanos(0)
        .build();
    com.google.protobuf.Timestamp expectedEndTimestamp = com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(endTimeInSeconds)
        .setNanos(0)
        .build();

    element.setField(0, startTimestamp);
    element.setField(1, endTimestamp);
    element.setField(2, 13);
    element.setField(3, 3322909458387959808L);
    element.setField(4, BIKE);
    element.setField(5, 2L);

    byte[] serializeValue = protoSerializer.serializeValue(element);

    AggregatedSupplyMessage actualValue = AggregatedSupplyMessage.parseFrom(serializeValue);

    Assert.assertEquals(expectedStartTimestamp, actualValue.getWindowStartTime());
    Assert.assertEquals(expectedEndTimestamp, actualValue.getWindowEndTime());
    Assert.assertEquals(13, actualValue.getS2IdLevel());
    Assert.assertEquals(3322909458387959808L, actualValue.getS2Id());
    Assert.assertEquals("BIKE", actualValue.getVehicleType().toString());
    Assert.assertEquals(2L, actualValue.getUniqueDrivers());
  }
}
