package com.tvd12.ezymq.kafka.endpoint;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.util.EzyLoggable;

public class EzyKafkaEndpoint extends EzyLoggable {

	@SuppressWarnings({"rawtypes", "unchecked"})
	public abstract static class Builder<B extends Builder<B>> 
			implements EzyBuilder<EzyKafkaEndpoint> {
		
		protected final Map<String, Object> properties;
		
		public Builder() {
			this.properties = new HashMap<>();
		}
		
		public B property(String key, Object value) {
			this.properties.put(key, value);
			return (B)this;
		}
		
		public B properties(Map<String, Object> properties) {
			this.properties.putAll(properties);
			return (B)this;
		}
		
		protected Producer newProducer() {
			return new KafkaProducer<>(properties);
		}
		
		protected Consumer newConsumer() {
			return new KafkaConsumer<>(properties);
		}
		
	}
	
}
