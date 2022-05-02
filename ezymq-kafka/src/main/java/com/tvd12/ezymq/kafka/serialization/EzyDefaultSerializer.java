package com.tvd12.ezymq.kafka.serialization;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class EzyDefaultSerializer implements Serializer<Object> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Object object) {
        if (object instanceof String) {
            return ((String) object).getBytes();
        }
        return (byte[]) object;
    }

    @Override
    public void close() {}
}

