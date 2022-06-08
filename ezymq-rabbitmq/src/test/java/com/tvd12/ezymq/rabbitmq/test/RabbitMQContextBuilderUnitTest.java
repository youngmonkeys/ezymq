package com.tvd12.ezymq.rabbitmq.test;

import com.tvd12.ezyfox.binding.EzyBindingContext;
import com.tvd12.ezyfox.binding.codec.EzyBindingEntityCodec;
import com.tvd12.ezyfox.codec.*;
import com.tvd12.ezyfox.collect.Sets;
import com.tvd12.ezyfox.exception.BadRequestException;
import com.tvd12.ezyfox.exception.NotFoundException;
import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.common.codec.EzyMQBytesDataCodec;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.rabbitmq.EzyRabbitMQProxy;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcProducer;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandler;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitSettings;
import com.tvd12.ezymq.rabbitmq.test.entity.FiboRequest2;
import com.tvd12.ezymq.rabbitmq.test.mockup.ConnectionFactoryMockup;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

public class RabbitMQContextBuilderUnitTest extends BaseTest {

    protected static EzyMessageSerializer newMessageSerializer() {
        return new MsgPackSimpleSerializer();
    }

    protected static EzyMessageDeserializer newMessageDeserializer() {
        return new MsgPackSimpleDeserializer();
    }

    @Test
    public void test() throws Exception {
        ConnectionFactoryMockup connectionFactory = new ConnectionFactoryMockup();
        EzyRabbitMQProxy context = EzyRabbitMQProxy.builder()
            .connectionFactory(connectionFactory)
            .scan("com.tvd12.ezymq.rabbitmq.test.entity")
            .mapRequestType("fibonacci", int.class)
            .mapRequestType("fibonacci2", FiboRequest2.class)
            .mapRequestType("test", String.class)
            .mapRequestType("", String.class)
            .settingsBuilder()
            .topicSettingBuilder("test")
            .exchange("rmqia-topic-exchange")
            .producerEnable(true)
            .producerRoutingKey("rmqia-topic-routing-key")
            .consumerEnable(true)
            .consumerQueueName("mqia-topic")
            .parent()
            .rpcProducerSettingBuilder("fibonacci")
            .defaultTimeout(300 * 1000)
            .exchange("rmqia-rpc-exchange")
            .requestQueueName("rmqia-rpc-queue")
            .requestRoutingKey("rmqia-rpc-routing-key")
            .replyQueueName("rmqia-rpc-client-queue")
            .replyRoutingKey("rmqia-rpc-client-routing-key")
            .parent()
            .rpcConsumerSettingBuilder("fibonacci")
            .requestQueueName("rmqia-rpc-queue")
            .exchange("rmqia-rpc-exchange")
            .replyRoutingKey("rmqia-rpc-client-routing-key")
            .addRequestHandler(
                "fibonacci",
                new EzyRabbitRequestHandler<Integer>() {
                    @Override
                    public Object handle(Integer value) {
                        if (value == 0) {
                            throw new NotFoundException("not found value 0");
                        }
                        if (value == -1) {
                            throw new BadRequestException(1, "value = -1 invalid");
                        }
                        if (value == -2) {
                            throw new IllegalArgumentException("value = -2 invalid");
                        }
                        if (value == -3) {
                            throw new UnsupportedOperationException("value = -3 not accepted");
                        }
                        if (value < -3) {
                            throw new IllegalStateException("server maintain");
                        }
                        return value + 3;
                    }
                }
            )
            .addRequestHandler(
                "fibonacci2",
                new EzyRabbitRequestHandler<Integer>() {
                    @Override
                    public Object handle(Integer request) {
                        return 1;
                    }
                }
            )
            .parent()
            .parent()
            .build();
        EzyRabbitTopic<String> topic = context.getTopic("test");
        topic.addConsumer(message -> System.out.println("topic message: " + message));
        topic.publish("hello topic");
        EzyRabbitRpcProducer producer = context.getRpcProducer("fibonacci");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1; ++i) {
            System.out.println("rabbit rpc start call: " + i);
            try {
                producer.fire("fibonacci", 50);
                producer.fire("fibonacci", 0);
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                int result = producer.call("fibonacci", 100, int.class);
                System.out.println("i = " + i + ", result = " + result);
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                producer.call("fibonacci", 0, int.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                producer.call("fibonacci", -1, int.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                producer.call("fibonacci", -2, int.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                producer.call("fibonacci", -3, int.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                producer.call("fibonacci", -4, int.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            producer.call(new Object(), int.class);
        } catch (Exception e) {
            assert e instanceof IllegalArgumentException;
        }
        try {
            producer.call(new FiboRequest2(), int.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            producer.fire(new Object());
        } catch (Exception e) {
            assert e instanceof IllegalArgumentException;
        }
        try {
            producer.fire(new FiboRequest2());
        } catch (Exception e) {
            e.printStackTrace();
        }
        Thread.sleep(100);
        System.out.println("elapsed = " + (System.currentTimeMillis() - start));
        context.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSetComponents() {
        EzyRabbitSettings settings = EzyRabbitSettings.builder()
            .build();
        EzyBindingContext bindingContext = EzyBindingContext.builder()
            .scan("com.tvd12.ezymq.rabbitmq.test.entity")
            .build();
        EzyMessageSerializer messageSerializer = newMessageSerializer();
        EzyMessageDeserializer messageDeserializer = newMessageDeserializer();
        EzyEntityCodec entityCodec = EzyBindingEntityCodec.builder()
            .marshaller(bindingContext.newMarshaller())
            .unmarshaller(bindingContext.newUnmarshaller())
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .build();
        EzyMQDataCodec dataCodec = EzyMQBytesDataCodec.builder()
            .marshaller(bindingContext.newMarshaller())
            .unmarshaller(bindingContext.newUnmarshaller())
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .mapRequestType("fibonacci", int.class)
            .mapRequestType("test", String.class)
            .build();
        EzyRabbitMQProxy proxy = EzyRabbitMQProxy.builder()
            .scan("com.tvd12.ezymq.rabbitmq.test.entity")
            .scan("com.tvd12.ezymq.rabbitmq.test.entity", "com.tvd12.ezymq.rabbitmq.test.entity")
            .scan(Sets.newHashSet("com.tvd12.ezymq.rabbitmq.test.entity"))
            .settings(settings)
            .bindingContext(bindingContext)
            .marshaller(bindingContext.newMarshaller())
            .unmarshaller(bindingContext.newUnmarshaller())
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .entityCodec(entityCodec)
            .dataCodec(dataCodec)
            .mapRequestTypes(EzyMapBuilder.mapBuilder()
                .put("a", int.class)
                .build())
            .build();
        proxy.close();
    }
}
