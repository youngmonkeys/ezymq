package com.tvd12.ezymq.kafka.setting;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezymq.kafka.EzyKafkaProxyBuilder;
import com.tvd12.ezymq.kafka.annotation.EzyKafkaHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandler;
import com.tvd12.properties.file.util.PropertiesUtil;

import lombok.Getter;

@Getter
public class EzyKafkaSettings {

	protected final Map<String, EzyKafkaProducerSetting> producerSettings;
	protected final Map<String, EzyKafkaConsumerSetting> consumerSettings;
	
	public static final String PRODUCERS_KEY = "kafka.producers";
	public static final String CONSUMERS_KEY = "kafka.consumers";
	public static final String TOPIC_KEY = "topic";
	public static final String THREAD_POOL_SIZE_KEY = "thread_pool_size";
	
	public EzyKafkaSettings(
			Map<String, EzyKafkaProducerSetting> callerSettings,
			Map<String, EzyKafkaConsumerSetting> handlerSettings) {
		this.producerSettings = Collections.unmodifiableMap(callerSettings);
		this.consumerSettings = Collections.unmodifiableMap(handlerSettings);
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	@SuppressWarnings("rawtypes")
	public static class Builder implements EzyBuilder<EzyKafkaSettings> {
		
		protected EzyKafkaProxyBuilder parent;
		protected Map<String, Object> properties;
		protected EzyKafkaMessageInterceptor consumerInterceptor;
		protected Map<String, EzyKafkaProducerSetting> producerSettings;
		protected Map<String, EzyKafkaConsumerSetting> consumerSettings;
		protected Map<String, EzyKafkaProducerSetting.Builder> producerSettingBuilders;
		protected Map<String, EzyKafkaConsumerSetting.Builder> consumerSettingBuilders;
		protected Map<String, Map<String, EzyKafkaMessageHandler>> consumerMessageHandlers;
		
		public Builder() {
			this(null);
		}
		
		public Builder(EzyKafkaProxyBuilder parent) {
			this.parent = parent;
			this.properties = new HashMap<>();
			this.producerSettings = new HashMap<>();
			this.consumerSettings = new HashMap<>();
			this.producerSettingBuilders = new HashMap<>();
			this.consumerSettingBuilders = new HashMap<>();
			this.consumerMessageHandlers = new HashMap<>();
		}
		
		public Builder property(String key, Object value) {
			this.properties.put(key, value);
			return this;
		}
		
		public Builder properties(Map<String, Object> properties) {
			this.properties.putAll(properties);
			return this;
		}
		
		public Builder consumerInterceptor(EzyKafkaMessageInterceptor consumerInterceptor) {
			this.consumerInterceptor = consumerInterceptor;
			return this;
		}
		
		public Builder consumerMessageHandlers(List<EzyKafkaMessageHandler> consumerMessageHandlers) {
			for(EzyKafkaMessageHandler handler : consumerMessageHandlers) {
				EzyKafkaHandler anno = handler.getClass().getAnnotation(EzyKafkaHandler.class);
				String topic = anno.topic();
				String command = anno.command();
				this.consumerMessageHandlers
					.computeIfAbsent(topic, k -> new HashMap<>())
					.put(command, handler);
			}
			return this;
		}
		
		public EzyKafkaProducerSetting.Builder producerSettingBuilder(String name) {
			return producerSettingBuilders.computeIfAbsent(
					name, k -> new EzyKafkaProducerSetting.Builder(this));
		}
		
		public EzyKafkaConsumerSetting.Builder consumerSettingBuilder(String name) {
			return consumerSettingBuilders.computeIfAbsent(
					name, k -> new EzyKafkaConsumerSetting.Builder(this));
		}
		
		public Builder addProducerSetting(String name, EzyKafkaProducerSetting setting) {
			this.producerSettings.put(name, setting);
			return this;
		}
		
		public Builder addConsumerSetting(String name, EzyKafkaConsumerSetting setting) {
			this.consumerSettings.put(name, setting);
			return this;
		}
		
		
		public EzyKafkaProxyBuilder parent() {
			return parent;
		}
		
		@SuppressWarnings({ "unchecked" })
		@Override
		public EzyKafkaSettings build() {
			String bootstrapServers = 
					(String)properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
			if(bootstrapServers == null)
				bootstrapServers = "localhost:9092";
			Properties producersProperties = 
					PropertiesUtil.getPropertiesByPrefix(properties, PRODUCERS_KEY);
			Set<String> producerNames = new HashSet<>();
			producerNames.addAll(producerSettingBuilders.keySet());
			producerNames.addAll(PropertiesUtil.getFirstPropertyKeys(producersProperties));
			for(String name : producerNames) {
				Properties producerProperties = 
						PropertiesUtil.getPropertiesByPrefix(producersProperties, name);
				if(!producerProperties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
					producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
				if(!producerProperties.containsKey(ProducerConfig.CLIENT_ID_CONFIG))
					producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, name);
				EzyKafkaProducerSetting.Builder builder = producerSettingBuilders.get(name);
				if(builder == null) {
					String topic = producerProperties.getProperty(TOPIC_KEY);
					if(topic == null)
						topic = name;
					builder = EzyKafkaProducerSetting.builder()
							.topic(topic);
				}
				builder.properties((Map) producerProperties);
				producerSettings.put(name, (EzyKafkaProducerSetting)builder.build());
			}
			Properties consumersProperties = 
					PropertiesUtil.getPropertiesByPrefix(properties, CONSUMERS_KEY);
			Set<String> consumerNames = new HashSet<>();
			consumerNames.addAll(consumerSettingBuilders.keySet());
			consumerNames.addAll(PropertiesUtil.getFirstPropertyKeys(consumersProperties));
			for(String name : consumerNames) {
				Properties consumerProperties = 
						PropertiesUtil.getPropertiesByPrefix(consumersProperties, name);
				if(!consumerProperties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
					consumerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
				if(!consumerProperties.containsKey(ConsumerConfig.GROUP_ID_CONFIG))
					consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, name);
				String topic = consumerProperties.getProperty(TOPIC_KEY);
				if(topic == null)
					topic = name;
				EzyKafkaConsumerSetting.Builder builder = consumerSettingBuilders.get(name);
				if(builder == null) {
					builder = EzyKafkaConsumerSetting.builder()
							.topic(topic);
					if(consumerProperties.containsKey(THREAD_POOL_SIZE_KEY))
						builder.threadPoolSize((int)consumerProperties.get(THREAD_POOL_SIZE_KEY));
				}
				builder.properties((Map) consumerProperties);
				builder.messageInterceptor(consumerInterceptor);
				builder.addMessageHandlers(consumerMessageHandlers.get(topic));
				consumerSettings.put(name, (EzyKafkaConsumerSetting)builder.build());
			}
			return new EzyKafkaSettings(producerSettings, consumerSettings);
		}
		
	}
	
}
