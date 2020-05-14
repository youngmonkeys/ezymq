package com.tvd12.ezymq.activemq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.ConnectionFactory;

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
import com.tvd12.ezymq.activemq.codec.EzyActiveBytesDataCodec;
import com.tvd12.ezymq.activemq.codec.EzyActiveBytesEntityCodec;
import com.tvd12.ezymq.activemq.codec.EzyActiveDataCodec;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveConnectionFactoryBuilder;
import com.tvd12.ezymq.activemq.setting.EzyActiveSettings;

@SuppressWarnings("rawtypes")
public class EzyActiveMQContextBuilder implements EzyBuilder<EzyActiveMQContext> {

	protected EzyActiveSettings settings;
	protected Set<String> packagesToScan;
	protected EzyMarshaller marshaller;
	protected EzyUnmarshaller unmarshaller;
	protected EzyEntityCodec entityCodec;
	protected EzyActiveDataCodec dataCodec;
	protected Map<String, Class> requestTypes;
	protected EzyBindingContext bindingContext;
	protected ConnectionFactory connectionFactory;
	protected EzyMessageSerializer messageSerializer;
	protected EzyMessageDeserializer messageDeserializer;
	protected List<EzyReflection> reflectionsToScan;
	protected EzyActiveSettings.Builder settingsBuilder;
	
	public EzyActiveMQContextBuilder() {
		this.requestTypes = new HashMap<>();
		this.packagesToScan = new HashSet<>();
		this.reflectionsToScan = new ArrayList<>();
	}
	
	public EzyActiveMQContextBuilder scan(String packageName) {
		this.packagesToScan.add(packageName);
		return this;
	}
	
	public EzyActiveMQContextBuilder scan(String... packageNames) {
		return scan(Arrays.asList(packageNames));
	}
	
	public EzyActiveMQContextBuilder scan(Iterable<String> packageNames) {
		for(String packageName : packageNames)
			scan(packageName);
		return this;
	}
	
	public EzyActiveMQContextBuilder scan(EzyReflection reflection) {
		this.reflectionsToScan.add(reflection);
		return this;
	}
	
	public EzyActiveSettings.Builder settingsBuilder() {
		if(settingsBuilder == null)
			settingsBuilder = new EzyActiveSettings.Builder(this);
		return settingsBuilder;
	}
	
	public EzyActiveMQContextBuilder settings(EzyActiveSettings settings) {
		this.settings = settings;
		return this;
	}
	
	public EzyActiveMQContextBuilder marshaller(EzyMarshaller marshaller) {
		this.marshaller = marshaller;
		return this;
	}
	
	public EzyActiveMQContextBuilder unmarshaller(EzyUnmarshaller unmarshaller) {
		this.unmarshaller = unmarshaller;
		return this;
	}
	
	public EzyActiveMQContextBuilder entityCodec(EzyEntityCodec entityCodec) {
		this.entityCodec = entityCodec;
		return this;
	}
	
	public EzyActiveMQContextBuilder dataCodec(EzyActiveDataCodec dataCodec) {
		this.dataCodec = dataCodec;
		return this;
	}
	
	public EzyActiveMQContextBuilder bindingContext(EzyBindingContext bindingContext) {
		this.bindingContext = bindingContext;
		return this;
	}
	
	public EzyActiveMQContextBuilder connectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
		return this;
	}
	
	public EzyActiveMQContextBuilder messageSerializer(EzyMessageSerializer messageSerializer) {
		this.messageSerializer = messageSerializer;
		return this;
	}
	
	public EzyActiveMQContextBuilder messageDeserializer(EzyMessageDeserializer messageDeserializer) {
		this.messageDeserializer = messageDeserializer;
		return this;
	}
	
	public EzyActiveMQContextBuilder mapRequestType(String cmd, Class<?> type) {
		this.requestTypes.put(cmd, type);
		return this;
	}
	
	public EzyActiveMQContextBuilder mapRequestTypes(Map<String, Class> requestTypes) {
		this.requestTypes.putAll(requestTypes);
		return this;
	}
	
	@Override
	public EzyActiveMQContext build() {
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
		if(connectionFactory == null)
			connectionFactory = new EzyActiveConnectionFactoryBuilder().build();
		return new EzyActiveMQContext(entityCodec, dataCodec, settings, connectionFactory);
	}
	
	protected EzyEntityCodec newEntityCodec() {
		return EzyActiveBytesEntityCodec.builder()
				.marshaller(marshaller)
				.unmarshaller(unmarshaller)
				.messageSerializer(messageSerializer)
				.messageDeserializer(messageDeserializer)
				.build();
	}
	
	protected EzyActiveDataCodec newDataCodec() {
		return EzyActiveBytesDataCodec.builder()
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
