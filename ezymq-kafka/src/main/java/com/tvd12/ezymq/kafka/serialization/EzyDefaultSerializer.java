package com.tvd12.ezymq.kafka.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

public class EzyDefaultSerializer implements Serializer<Object> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public byte[] serialize(String topic, Object object) {
		return (byte[])object;
	}
	
	@Override
	public void close() {
	}
	
}

