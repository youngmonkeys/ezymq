package com.tvd12.ezymq.kafka.codec;

import java.util.HashMap;
import java.util.Map;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.builder.EzyBuilder;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class EzyKafkaAbstractDataCodecBuilder<B extends EzyKafkaAbstractDataCodecBuilder> 
		implements EzyBuilder<EzyKafkaDataCodec> {

	protected EzyMarshaller marshaller;
	protected EzyUnmarshaller unmarshaller;
	protected Map<String, Map<String, Class>> messageTypesByTopic = new HashMap<>();
	
	public B marshaller(EzyMarshaller marshaller) {
		this.marshaller = marshaller;
		return (B)this;
	}
	
	public B unmarshaller(EzyUnmarshaller unmarshaller) {
		this.unmarshaller = unmarshaller;
		return (B)this;
	}
	
	public B mapMessageType(String topic, Class messageType) {
		return mapMessageType(topic, "", messageType);
	}
	
	public B mapMessageType(String topic, String cmd, Class messageType) {
		this.messageTypesByTopic.computeIfAbsent(topic, k -> new HashMap<>())
			.put(cmd, messageType);
	return (B)this;
	}
	
	public B mapMessageTypes(String topic, Map<String, Class> messageTypeByCommand) {
		this.messageTypesByTopic.computeIfAbsent(topic, k -> new HashMap<>())
			.putAll(messageTypeByCommand);
		return (B)this;
	}
	
	public B mapMessageTypes(Map<String, Map<String, Class>> messageTypesByTopic) {
		this.messageTypesByTopic.putAll(messageTypesByTopic);
		return (B)this;
	}
	
	@Override
	public EzyKafkaDataCodec build() {
		EzyKafkaAbstractDataCodec product = newProduct();
		product.setMarshaller(marshaller);
		product.setUnmarshaller(unmarshaller);
		product.setMessageTypesByTopic(messageTypesByTopic);
		return product;
	}
	
	protected abstract EzyKafkaAbstractDataCodec newProduct();
	
}
