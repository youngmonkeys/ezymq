package com.tvd12.ezymq.activemq.test;

import javax.jms.ConnectionFactory;

import com.tvd12.ezyfox.binding.EzyBindingContext;
import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.binding.impl.EzySimpleBindingContext;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.codec.EzyMessageDeserializer;
import com.tvd12.ezyfox.codec.EzyMessageSerializer;
import com.tvd12.ezyfox.codec.MsgPackSimpleDeserializer;
import com.tvd12.ezyfox.codec.MsgPackSimpleSerializer;
import com.tvd12.ezymq.activemq.codec.EzyActiveBytesDataCodec;
import com.tvd12.ezymq.activemq.codec.EzyActiveBytesEntityCodec;
import com.tvd12.ezymq.activemq.codec.EzyActiveDataCodec;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveConnectionFactoryBuilder;

public class ActiveMQBaseTest {

	protected static EzyMarshaller marshaller;
	protected static EzyUnmarshaller unmarshaller;
	protected static EzyMessageSerializer messageSerializer;
	protected static EzyMessageDeserializer messageDeserializer;

	protected static EzyEntityCodec entityCodec;
	protected static EzyActiveDataCodec dataCodec;
	protected static EzyBindingContext bindingContext;
	
	protected static ConnectionFactory connectionFactory;
	
	static {
		connectionFactory = new EzyActiveConnectionFactoryBuilder()
				.build();
		messageSerializer = newMessageSerializer();
		messageDeserializer = newMessageDeserializer();
		bindingContext = newBindingContext();
		marshaller = bindingContext.newMarshaller();
		unmarshaller = bindingContext.newUnmarshaller();
		entityCodec = EzyActiveBytesEntityCodec.builder()
				.marshaller(marshaller)
				.unmarshaller(unmarshaller)
				.messageSerializer(messageSerializer)
				.messageDeserializer(messageDeserializer)
				.build();
		dataCodec = EzyActiveBytesDataCodec.builder()
				.marshaller(marshaller)
				.unmarshaller(unmarshaller)
				.messageSerializer(messageSerializer)
				.messageDeserializer(messageDeserializer)
				.mapRequestType("fibonaci", int.class)
				.mapRequestType("test", String.class)
				.build();
	}
	
	protected static EzyMessageSerializer newMessageSerializer() {
		return new MsgPackSimpleSerializer();
	}
	
	protected static EzyMessageDeserializer newMessageDeserializer() {
		return new MsgPackSimpleDeserializer();
	}
	
	private static EzyBindingContext newBindingContext() {
		return EzySimpleBindingContext.builder()
				.scan("com.tvd12.ezymq.activemq.test.entity")
				.build();
	}
	
}
