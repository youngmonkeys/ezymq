package com.tvd12.ezymq.kafka.setting;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezymq.kafka.EzyKafkaContextBuilder;

import lombok.Getter;

@Getter
public class EzyKafkaSettings {

	protected final Map<String, EzyKafkaCallerSetting> callerSettings;
	protected final Map<String, EzyKafkaHandlerSetting> handlerSettings;
	
	public EzyKafkaSettings(
			Map<String, EzyKafkaCallerSetting> callerSettings,
			Map<String, EzyKafkaHandlerSetting> handlerSettings) {
		this.callerSettings = Collections.unmodifiableMap(callerSettings);
		this.handlerSettings = Collections.unmodifiableMap(handlerSettings);
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder implements EzyBuilder<EzyKafkaSettings> {
		
		protected EzyKafkaContextBuilder parent;
		protected Map<String, Object> properties;
		protected Map<String, EzyKafkaCallerSetting> callerSettings;
		protected Map<String, EzyKafkaHandlerSetting> handlerSettings;
		protected Map<String, EzyKafkaCallerSetting.Builder> callerSettingBuilders;
		protected Map<String, EzyKafkaHandlerSetting.Builder> handlerSettingBuilders;
		
		public Builder() {
			this(null);
		}
		
		public Builder(EzyKafkaContextBuilder parent) {
			this.parent = parent;
			this.properties = new HashMap<>();
			this.callerSettings = new HashMap<>();
			this.handlerSettings = new HashMap<>();
			this.callerSettingBuilders = new HashMap<>();
			this.handlerSettingBuilders = new HashMap<>();
		}
		
		public Builder property(String key, Object value) {
			this.properties.put(key, value);
			return this;
		}
		
		public Builder properties(Map<String, Object> properties) {
			this.properties.putAll(properties);
			return this;
		}
		
		public EzyKafkaCallerSetting.Builder callerSettingBuilder(String name) {
			return callerSettingBuilders.computeIfAbsent(
					name, k -> new EzyKafkaCallerSetting.Builder(this));
		}
		
		public EzyKafkaHandlerSetting.Builder handlerSettingBuilder(String name) {
			return handlerSettingBuilders.computeIfAbsent(
					name, k -> new EzyKafkaHandlerSetting.Builder(this));
		}
		
		public Builder addRpcCallerSetting(String name, EzyKafkaCallerSetting setting) {
			this.callerSettings.put(name, setting);
			return this;
		}
		
		public Builder addRpcHandlerSetting(String name, EzyKafkaHandlerSetting setting) {
			this.handlerSettings.put(name, setting);
			return this;
		}
		
		
		public EzyKafkaContextBuilder parent() {
			return parent;
		}
		
		@Override
		public EzyKafkaSettings build() {
			for(String name : callerSettingBuilders.keySet()) {
				EzyKafkaCallerSetting.Builder builder = callerSettingBuilders.get(name);
				builder.properties(properties);
				callerSettings.put(name, (EzyKafkaCallerSetting)builder.build());
			}
			for(String name : handlerSettingBuilders.keySet()) {
				EzyKafkaHandlerSetting.Builder builder = handlerSettingBuilders.get(name);
				builder.properties(properties);
				handlerSettings.put(name, (EzyKafkaHandlerSetting)builder.build());
			}
			return new EzyKafkaSettings(callerSettings, handlerSettings);
		}
		
	}
	
}
