package com.tvd12.ezymq.kafka.codec;

import java.util.Map;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;

import lombok.Setter;

@Setter
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class EzyKafkaAbstractDataCodec implements EzyKafkaDataCodec {

	protected EzyMarshaller marshaller;
	protected EzyUnmarshaller unmarshaller;
	protected Map<String, Map<String, Class>> messageTypesByTopic;
	
	public EzyKafkaAbstractDataCodec() {}
	
	public EzyKafkaAbstractDataCodec(
			EzyMarshaller marshaller,
			EzyUnmarshaller unmarshaller,
			Map<String, Map<String, Class>> messageTypesByTopic) {
		this.marshaller = marshaller;
		this.unmarshaller = unmarshaller;
		this.messageTypesByTopic = messageTypesByTopic;
	}
	
	protected Object marshallEntity(Object entity) {
		Object answer = marshaller.marshal(entity);
		return answer;
	}
	
	protected Object unmarshallData(String topic, String cmd, Object value) {
		Map<String, Class> messageTypeByCommand = messageTypesByTopic.get(topic);
		if(messageTypeByCommand == null)
			throw new IllegalArgumentException("has no message type mapped to topic: " + topic + " and command: " + cmd);
		Class messageType = messageTypeByCommand.get(cmd);
		if(messageType == null)
			throw new IllegalArgumentException("has no message type mapped to topic: " + topic + " and command: " + cmd);
		Object answer = unmarshaller.unmarshal(value, messageType);
		return answer;
	}
	
}
