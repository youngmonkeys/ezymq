package com.tvd12.ezymq.kafka.setting;

import java.util.Map;

import org.apache.kafka.clients.producer.Producer;

import lombok.Getter;

@Getter
@SuppressWarnings("rawtypes")
public class EzyKafkaProducerSetting extends EzyKafkaEndpointSetting {

	protected final Producer producer;
    
    public EzyKafkaProducerSetting(
    		String topic,
    		Producer producer, Map<String, Object> properties) {
    	super(topic, properties);
    	this.producer = producer;
	}
    
    public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder extends EzyKafkaEndpointSetting.Builder<Builder> {
		
		protected Producer producer;
		protected EzyKafkaSettings.Builder parent;
		
		public Builder() {
			this(null);
		}
		
		public Builder(EzyKafkaSettings.Builder parent) {
			super();
			this.parent = parent;
		}
		
		public Builder producer(Producer producer) {
			this.producer = producer;
			return this;
		}
		
		public EzyKafkaSettings.Builder parent() {
			return parent;
		}
		
		@Override
		public EzyKafkaProducerSetting build() {
			if(topic == null)
				throw new NullPointerException("topic can not be null");
			return new EzyKafkaProducerSetting(topic, producer, properties);
		}
		
	}
}
