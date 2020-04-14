package com.tvd12.ezymq.activemq.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.tvd12.ezyfox.builder.EzyBuilder;

import lombok.Getter;

@Getter
public class EzyActiveProperties {

	protected String type;
	protected String correlationId;
	protected Map<String, Object> properties;
	
	protected EzyActiveProperties(Builder builder) {
		this.type = builder.type;
		this.correlationId = builder.correlationId;
		this.properties = builder.properties;
	}
	
	public Set<String> keySet() {
		if(properties == null)
			return Collections.emptySet();
		return properties.keySet();
	}
	
	public Object getValue(String key) {
		Object value = null;
		if(properties == null)
			value = properties.get(key);
		return value;
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
			.append("(")
				.append("type: ").append(type).append(", ")
				.append("correlationId: ").append(correlationId).append(", ")
				.append("properties: ").append(properties)
			.append(")")
			.toString();
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder implements EzyBuilder<EzyActiveProperties> {
		
		protected String type;
		protected String correlationId;
		protected Map<String, Object> properties;
		
		public Builder type(String type) {
			this.type = type;
			return this;
		}
		
		public Builder correlationId(String correlationId) {
			this.correlationId = correlationId;
			return this;
		}
		
		public Builder addProperty(String key, Object value) {
			if(properties == null)
				properties = new HashMap<>();
			properties.put(key, value);
			return this;
		}
		
		public Builder addProperties(Map<String, Object> props) {
			if(properties == null)
				properties = new HashMap<>();
			properties.putAll(props);
			return this;
		}
		
		public Builder addProperties(EzyActiveProperties props) {
			if(props == null)
				return this;
			this.type = props.type;
			this.correlationId = props.correlationId;
			return this.addProperties(props.properties);
		}
		
		@Override
		public EzyActiveProperties build() {
			return new EzyActiveProperties(this);
		}
		
	}
}
