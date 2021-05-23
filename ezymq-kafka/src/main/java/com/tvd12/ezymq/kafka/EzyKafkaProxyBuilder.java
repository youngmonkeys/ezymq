package com.tvd12.ezymq.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tvd12.ezyfox.binding.EzyBindingContext;
import com.tvd12.ezyfox.binding.EzyBindingContextBuilder;
import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.binding.impl.EzySimpleBindingContext;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.codec.EzyMessageDeserializer;
import com.tvd12.ezyfox.codec.EzyMessageSerializer;
import com.tvd12.ezyfox.codec.MsgPackSimpleDeserializer;
import com.tvd12.ezyfox.codec.MsgPackSimpleSerializer;
import com.tvd12.ezyfox.reflect.EzyReflection;
import com.tvd12.ezyfox.reflect.EzyReflectionProxy;
import com.tvd12.ezymq.kafka.codec.EzyKafkaBytesDataCodec;
import com.tvd12.ezymq.kafka.codec.EzyKafkaBytesEntityCodec;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.setting.EzyKafkaSettings;

@SuppressWarnings("rawtypes")
public class EzyKafkaProxyBuilder implements EzyBuilder<EzyKafkaProxy> {

	protected EzyKafkaSettings settings;
	protected Set<String> packagesToScan;
	protected EzyMarshaller marshaller;
	protected EzyUnmarshaller unmarshaller;
	protected EzyEntityCodec entityCodec;
	protected EzyKafkaDataCodec dataCodec;
	protected Map<String, Class> requestTypes;
	protected EzyBindingContext bindingContext;
	protected EzyMessageSerializer messageSerializer;
	protected EzyMessageDeserializer messageDeserializer;
	protected List<EzyReflection> reflectionsToScan;
	protected EzyKafkaSettings.Builder settingsBuilder;
	
	public EzyKafkaProxyBuilder() {
		this.requestTypes = new HashMap<>();
		this.packagesToScan = new HashSet<>();
		this.reflectionsToScan = new ArrayList<>();
	}
	
	public EzyKafkaProxyBuilder scan(String packageName) {
		this.packagesToScan.add(packageName);
		return this;
	}
	
	public EzyKafkaProxyBuilder scan(String... packageNames) {
		return scan(Arrays.asList(packageNames));
	}
	
	public EzyKafkaProxyBuilder scan(Iterable<String> packageNames) {
		for(String packageName : packageNames)
			scan(packageName);
		return this;
	}
	
	public EzyKafkaProxyBuilder scan(EzyReflection reflection) {
		this.reflectionsToScan.add(reflection);
		return this;
	}
	
	public EzyKafkaSettings.Builder settingsBuilder() {
		if(settingsBuilder == null)
			settingsBuilder = new EzyKafkaSettings.Builder(this);
		return settingsBuilder;
	}
	
	public EzyKafkaProxyBuilder settings(EzyKafkaSettings settings) {
		this.settings = settings;
		return this;
	}
	
	public EzyKafkaProxyBuilder marshaller(EzyMarshaller marshaller) {
		this.marshaller = marshaller;
		return this;
	}
	
	public EzyKafkaProxyBuilder unmarshaller(EzyUnmarshaller unmarshaller) {
		this.unmarshaller = unmarshaller;
		return this;
	}
	
	public EzyKafkaProxyBuilder entityCodec(EzyEntityCodec entityCodec) {
		this.entityCodec = entityCodec;
		return this;
	}
	
	public EzyKafkaProxyBuilder dataCodec(EzyKafkaDataCodec dataCodec) {
		this.dataCodec = dataCodec;
		return this;
	}
	
	public EzyKafkaProxyBuilder bindingContext(EzyBindingContext bindingContext) {
		this.bindingContext = bindingContext;
		return this;
	}
	
	public EzyKafkaProxyBuilder messageSerializer(EzyMessageSerializer messageSerializer) {
		this.messageSerializer = messageSerializer;
		return this;
	}
	
	public EzyKafkaProxyBuilder messageDeserializer(EzyMessageDeserializer messageDeserializer) {
		this.messageDeserializer = messageDeserializer;
		return this;
	}
	
	public EzyKafkaProxyBuilder mapRequestType(String cmd, Class<?> type) {
		this.requestTypes.put(cmd, type);
		return this;
	}
	
	public EzyKafkaProxyBuilder mapRequestTypes(Map<String, Class> requestTypes) {
		this.requestTypes.putAll(requestTypes);
		return this;
	}
	
	@Override
	public EzyKafkaProxy build() {
		if(settingsBuilder != null)
			settings = settingsBuilder.build();
		if(settings == null)
			throw new NullPointerException("settings can not be null");
		if(bindingContext == null)
			bindingContext = newBindingContext();
		if(bindingContext != null) {
			marshaller = bindingContext.newMarshaller();
			unmarshaller = bindingContext.newUnmarshaller();
		}
		if(marshaller == null)
			throw new IllegalStateException("marshaller is null, set its or set bindingContext or add package to scan");
		if(unmarshaller == null)
			throw new IllegalStateException("unmarshaller is null, set its or set bindingContext or add package to scan");
		if(messageSerializer == null)
			messageSerializer = newMessageSerializer();
		if(messageDeserializer == null)
			messageDeserializer = newMessageDeserializer();
		if(dataCodec == null)
			dataCodec = newDataCodec();
		if(entityCodec == null)
			entityCodec = newEntityCodec();
		return new EzyKafkaProxy(entityCodec, dataCodec, settings);
	}
	
	protected EzyEntityCodec newEntityCodec() {
		return EzyKafkaBytesEntityCodec.builder()
				.marshaller(marshaller)
				.unmarshaller(unmarshaller)
				.messageSerializer(messageSerializer)
				.messageDeserializer(messageDeserializer)
				.build();
	}
	
	protected EzyKafkaDataCodec newDataCodec() {
		return EzyKafkaBytesDataCodec.builder()
				.marshaller(marshaller)
				.unmarshaller(unmarshaller)
				.messageSerializer(messageSerializer)
				.messageDeserializer(messageDeserializer)
				.mapRequestTypes(requestTypes)
				.build();
	}
	
	protected EzyMessageSerializer newMessageSerializer() {
		return new MsgPackSimpleSerializer();
	}
	
	protected EzyMessageDeserializer newMessageDeserializer() {
		return new MsgPackSimpleDeserializer();
	}
	
	private EzyBindingContext newBindingContext() {
		if(packagesToScan.size() > 0)
			reflectionsToScan.add(new EzyReflectionProxy(packagesToScan));
		if(reflectionsToScan.isEmpty())
			return null;
		EzyBindingContextBuilder builder = EzySimpleBindingContext.builder();
		for(EzyReflection reflection : reflectionsToScan)
			builder.addAllClasses(reflection);
		return builder.build();
	}
	
}
