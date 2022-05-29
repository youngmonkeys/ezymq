package com.tvd12.ezymq.rabbitmq.test;

import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezyfox.binding.EzyBindingContext;
import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.binding.codec.EzyBindingEntityCodec;
import com.tvd12.ezyfox.binding.impl.EzySimpleBindingContext;
import com.tvd12.ezyfox.codec.*;
import com.tvd12.ezymq.common.codec.EzyMQBytesDataCodec;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitConnectionFactoryBuilder;
import com.tvd12.test.base.BaseTest;

public class RabbitBaseTest extends BaseTest {

    protected static EzyMarshaller marshaller;
    protected static EzyUnmarshaller unmarshaller;
    protected static EzyMessageSerializer messageSerializer;
    protected static EzyMessageDeserializer messageDeserializer;

    protected static EzyMQDataCodec dataCodec;
    protected static EzyEntityCodec entityCodec;
    protected static EzyBindingContext bindingContext;

    protected static ConnectionFactory connectionFactory;

    static {
        connectionFactory = new EzyRabbitConnectionFactoryBuilder()
            .sharedThreadPoolSize(1)
            .maxConnectionAttempts(Integer.MAX_VALUE)
            .build();
        messageSerializer = newMessageSerializer();
        messageDeserializer = newMessageDeserializer();
        bindingContext = newBindingContext();
        marshaller = bindingContext.newMarshaller();
        unmarshaller = bindingContext.newUnmarshaller();
        entityCodec = EzyBindingEntityCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .build();
        dataCodec = EzyMQBytesDataCodec.builder()
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
            .scan("com.tvd12.ezymq.rabbitmq.test.entity")
            .build();
    }
}
