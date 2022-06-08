package com.tvd12.ezymq.rabbitmq.test.endpoint;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcClient;
import com.tvd12.ezymq.rabbitmq.factory.EzyRabbitCorrelationIdFactory;
import com.tvd12.ezymq.rabbitmq.factory.EzyRabbitSimpleCorrelationIdFactory;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitResponseConsumer;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class EzyRabbitRpcClientTest extends BaseTest {

    @Test
    public void doFireWithPropsIsNull() throws IOException {
        // given
        Channel channel = mock(Channel.class);
        String exchange = RandomUtil.randomShortAlphabetString();
        String requestRoutingKey = RandomUtil.randomShortAlphabetString();
        String routingKey = RandomUtil.randomShortAlphabetString();
        String replyQueueName = RandomUtil.randomShortAlphabetString();
        String replyRoutingKey = RandomUtil.randomShortAlphabetString();

        EzyRabbitRpcClient sut = EzyRabbitRpcClient.builder()
            .channel(channel)
            .exchange(exchange)
            .routingKey(requestRoutingKey)
            .replyRoutingKey(routingKey)
            .replyQueueName(replyQueueName)
            .replyRoutingKey(replyRoutingKey)
            .build();

        byte[] message = RandomUtil.randomShortByteArray();

        // when
        sut.doFire(null, message);

        // then
        verify(channel, times(1)).basicPublish(
            exchange,
            requestRoutingKey,
            new AMQP.BasicProperties.Builder().build(),
            message
        );
    }

    @Test
    public void handleDeliveryWithFeatureIsNullAndUnconsumedResponseConsumerIsNotNull() throws IOException {
        // given
        Channel channel = mock(Channel.class);
        String exchange = RandomUtil.randomShortAlphabetString();
        String requestRoutingKey = RandomUtil.randomShortAlphabetString();
        String routingKey = RandomUtil.randomShortAlphabetString();
        String replyQueueName = RandomUtil.randomShortAlphabetString();
        String replyRoutingKey = RandomUtil.randomShortAlphabetString();

        EzyRabbitResponseConsumer responseConsumer = mock(EzyRabbitResponseConsumer.class);

        EzyRabbitRpcClient sut = EzyRabbitRpcClient.builder()
            .channel(channel)
            .exchange(exchange)
            .routingKey(requestRoutingKey)
            .replyRoutingKey(routingKey)
            .replyQueueName(replyQueueName)
            .replyRoutingKey(replyRoutingKey)
            .unconsumedResponseConsumer(responseConsumer)
            .build();

        DefaultConsumer consumer = FieldUtil.getFieldValue(sut, "consumer");

        // when
        String consumerTag = RandomUtil.randomShortAlphabetString();
        Envelope envelope = mock(Envelope.class);
        String correlationId = RandomUtil.randomShortAlphabetString();
        AMQP.BasicProperties properties = new AMQP.BasicProperties()
            .builder()
            .correlationId(correlationId)
            .build();
        byte[] body = RandomUtil.randomShortByteArray();
        consumer.handleDelivery(consumerTag, envelope, properties, body);

        // then
        verify(responseConsumer, times(1)).consume(
            properties,
            body
        );
    }

    @Test
    public void handleDeliveryWithFeatureIsNullAndUnconsumedResponseConsumerIsNull() throws IOException {
        // given
        Channel channel = mock(Channel.class);
        String exchange = RandomUtil.randomShortAlphabetString();
        String requestRoutingKey = RandomUtil.randomShortAlphabetString();
        String routingKey = RandomUtil.randomShortAlphabetString();
        String replyQueueName = RandomUtil.randomShortAlphabetString();
        String replyRoutingKey = RandomUtil.randomShortAlphabetString();
        EzyRabbitCorrelationIdFactory correlationIdFactory = new
            EzyRabbitSimpleCorrelationIdFactory();

        EzyRabbitRpcClient sut = EzyRabbitRpcClient.builder()
            .channel(channel)
            .exchange(exchange)
            .routingKey(requestRoutingKey)
            .replyRoutingKey(routingKey)
            .replyQueueName(replyQueueName)
            .replyRoutingKey(replyRoutingKey)
            .correlationIdFactory(correlationIdFactory)
            .build();

        DefaultConsumer consumer = FieldUtil.getFieldValue(sut, "consumer");

        // when
        // then
        String consumerTag = RandomUtil.randomShortAlphabetString();
        Envelope envelope = mock(Envelope.class);
        String correlationId = RandomUtil.randomShortAlphabetString();
        AMQP.BasicProperties properties = new AMQP.BasicProperties()
            .builder()
            .correlationId(correlationId)
            .build();
        byte[] body = RandomUtil.randomShortByteArray();
        consumer.handleDelivery(consumerTag, envelope, properties, body);
    }
}
