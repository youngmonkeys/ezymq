package com.tvd12.ezymq.rabbitmq.testing;

import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezyfox.binding.EzyBindingContext;
import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.binding.impl.EzySimpleBindingContext;
import com.tvd12.ezyfox.codec.*;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitBytesDataCodec;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitBytesEntityCodec;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitDataCodec;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitConnectionFactoryBuilder;
import com.tvd12.test.base.BaseTest;

public class RabbitBaseTest extends BaseTest {

    protected static EzyMarshaller marshaller;
    protected static EzyUnmarshaller unmarshaller;
    protected static EzyMessageSerializer messageSerializer;
    protected static EzyMessageDeserializer messageDeserializer;

    protected static EzyEntityCodec entityCodec;
    protected static EzyRabbitDataCodec dataCodec;
    protected static EzyBindingContext bindingContext;

    protected static ConnectionFactory connectionFactory;

    static {
        connectionFactory = new EzyRabbitConnectionFactoryBuilder()
            .sharedThreadPoolSize(1)
            .build();
        messageSerializer = newMessageSerializer();
        messageDeserializer = newMessageDeserializer();
        bindingContext = newBindingContext();
        marshaller = bindingContext.newMarshaller();
        unmarshaller = bindingContext.newUnmarshaller();
        entityCodec = EzyRabbitBytesEntityCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .build();
        dataCodec = EzyRabbitBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .mapRequestType("fibonacci", int.class)
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
            .scan("com.tvd12.ezymq.rabbitmq.testing.entity")
            .build();
    }
}
