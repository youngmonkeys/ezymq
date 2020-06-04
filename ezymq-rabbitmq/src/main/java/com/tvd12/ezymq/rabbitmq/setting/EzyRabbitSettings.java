package com.tvd12.ezymq.rabbitmq.setting;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezymq.rabbitmq.EzyRabbitMQContextBuilder;

import lombok.Getter;

@Getter
public class EzyRabbitSettings {

	protected final Map<String, Map<String, Object>> queueArguments;
	protected final Map<String, EzyRabbitTopicSetting> topicSettings;
	protected final Map<String, EzyRabbitRpcCallerSetting> rpcCallerSettings;
	protected final Map<String, EzyRabbitRpcHandlerSetting> rpcHandlerSettings;
	
	public EzyRabbitSettings(
			Map<String, Map<String, Object>> queueArguments,
			Map<String, EzyRabbitTopicSetting> topicSettings,
			Map<String, EzyRabbitRpcCallerSetting> rpcCallerSettings,
			Map<String, EzyRabbitRpcHandlerSetting> rpcHandlerSettings) {
		this.queueArguments = Collections.unmodifiableMap(queueArguments);
		this.topicSettings = Collections.unmodifiableMap(topicSettings);
		this.rpcCallerSettings = Collections.unmodifiableMap(rpcCallerSettings);
		this.rpcHandlerSettings = Collections.unmodifiableMap(rpcHandlerSettings);
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder implements EzyBuilder<EzyRabbitSettings> {
		
		protected EzyRabbitMQContextBuilder parent;
		protected Map<String, Map<String, Object>> queueArguments;
		protected Map<String, EzyRabbitTopicSetting> topicSettings;
		protected Map<String, EzyRabbitRpcCallerSetting> rpcCallerSettings;
		protected Map<String, EzyRabbitRpcHandlerSetting> rpcHandlerSettings;
		protected Map<String, EzyRabbitTopicSetting.Builder> topicSettingBuilders;
		protected Map<String, EzyRabbitRpcCallerSetting.Builder> rpcCallerSettingBuilders;
		protected Map<String, EzyRabbitRpcHandlerSetting.Builder> rpcHandlerSettingBuilders;
		
		public Builder() {
			this(null);
		}
		
		public Builder(EzyRabbitMQContextBuilder parent) {
			this.parent = parent;
			this.topicSettings = new HashMap<>();
			this.queueArguments = new HashMap<>();
			this.rpcCallerSettings = new HashMap<>();
			this.rpcHandlerSettings = new HashMap<>();
			this.topicSettingBuilders = new HashMap<>();
			this.rpcCallerSettingBuilders = new HashMap<>();
			this.rpcHandlerSettingBuilders = new HashMap<>();
		}
		
		public Builder queueArgument(String queue, String key, Object value) {
			this.queueArguments.computeIfAbsent(queue, k -> new HashMap<>())
				.put(key, value);
			return this;
		}
		
		public Builder queueArguments(String queue, Map<String, Object> args) {
			this.queueArguments.computeIfAbsent(queue, k -> new HashMap<>())
				.putAll(args);
			return this;
		}
		
		public EzyRabbitTopicSetting.Builder topicSettingBuilder(String name) {
			return this.topicSettingBuilders.computeIfAbsent(
					name, k -> new EzyRabbitTopicSetting.Builder(this));
		}
		
		public EzyRabbitRpcCallerSetting.Builder rpcCallerSettingBuilder(String name) {
			return this.rpcCallerSettingBuilders.computeIfAbsent(
					name, k -> new EzyRabbitRpcCallerSetting.Builder(this));
		}
		
		public EzyRabbitRpcHandlerSetting.Builder rpcHandlerSettingBuilder(String name) {
			return this.rpcHandlerSettingBuilders.computeIfAbsent(
					name, k -> new EzyRabbitRpcHandlerSetting.Builder(this));
		}
		
		public Builder addTopicSetting(String name, EzyRabbitTopicSetting setting) {
			this.topicSettings.put(name, setting);
			return this;
		}
		
		public Builder addRpcCallerSetting(String name, EzyRabbitRpcCallerSetting setting) {
			this.rpcCallerSettings.put(name, setting);
			return this;
		}
		
		public Builder addRpcHandlerSetting(String name, EzyRabbitRpcHandlerSetting setting) {
			this.rpcHandlerSettings.put(name, setting);
			return this;
		}
		
		
		public EzyRabbitMQContextBuilder parent() {
			return parent;
		}
		
		@Override
		public EzyRabbitSettings build() {
			for(String name : topicSettingBuilders.keySet()) {
				EzyRabbitTopicSetting.Builder builder = topicSettingBuilders.get(name);
				topicSettings.put(name, (EzyRabbitTopicSetting)builder.build());
			}
			for(String name : rpcCallerSettingBuilders.keySet()) {
				EzyRabbitRpcCallerSetting.Builder builder = rpcCallerSettingBuilders.get(name);
				rpcCallerSettings.put(name, (EzyRabbitRpcCallerSetting)builder.build());
			}
			for(String name : rpcHandlerSettingBuilders.keySet()) {
				EzyRabbitRpcHandlerSetting.Builder builder = rpcHandlerSettingBuilders.get(name);
				rpcHandlerSettings.put(name, (EzyRabbitRpcHandlerSetting)builder.build());
			}
			return new EzyRabbitSettings(
					queueArguments, topicSettings, rpcCallerSettings, rpcHandlerSettings);
		}
		
	}
	
}
