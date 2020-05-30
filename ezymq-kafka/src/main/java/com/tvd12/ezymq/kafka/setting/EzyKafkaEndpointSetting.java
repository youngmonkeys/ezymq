package com.tvd12.ezymq.kafka.setting;

import java.util.HashMap;
import java.util.Map;

import com.tvd12.ezyfox.builder.EzyBuilder;

import lombok.Getter;

@Getter
public class EzyKafkaEndpointSetting {

	protected final Map<String, Object> properties;
	
	public EzyKafkaEndpointSetting(Map<String, Object> properties) {
		this.properties = properties;
	}
	
	@SuppressWarnings("unchecked")
	public static abstract class Builder<B extends Builder<B>> 
			implements EzyBuilder<EzyKafkaEndpointSetting> {
		
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

	}
	
}
