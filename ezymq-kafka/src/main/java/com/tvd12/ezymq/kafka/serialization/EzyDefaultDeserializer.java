package com.tvd12.ezymq.kafka.serialization;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class EzyDefaultDeserializer implements Deserializer<Object> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Object deserialize(String topic, byte[] data) {
        return data;
    }

    @Override
    public void close() {}
}
