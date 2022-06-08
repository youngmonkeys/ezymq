package com.tvd12.ezymq.rabbitmq.test.endpoint;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicServer;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitMessageHandler;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class EzyRabbitTopicServerTest extends BaseTest {

    @Test
    public void fetchQueueNameWithQueueNameIsNull() throws IOException {
        // given
        Channel channel = mock(Channel.class);
        String exchange = RandomUtil.randomShortAlphabetString();
        String queueName = RandomUtil.randomShortAlphabetString();

        AMQP.Queue.DeclareOk declareOk = mock(AMQP.Queue.DeclareOk.class);
        when(channel.queueDeclare()).thenReturn(declareOk);

        when(declareOk.getQueue()).thenReturn(queueName);

        // when
        EzyRabbitTopicServer sut = EzyRabbitTopicServer.builder()
            .channel(channel)
            .exchange(exchange)
            .build();

        // then
        Asserts.assertEquals(
            FieldUtil.getFieldValue(sut, "queueName"),
            queueName
        );

        verify(channel, times(1)).queueDeclare();
        verify(declareOk, times(1)).getQueue();
    }

    @Test
    public void handleDeliveryTest() throws IOException {
        // given
        Channel channel = mock(Channel.class);
        String exchange = RandomUtil.randomShortAlphabetString();
        String queueName = RandomUtil.randomShortAlphabetString();

        String consumerTag = RandomUtil.randomShortAlphabetString();
        Envelope envelope = mock(Envelope.class);
        AMQP.BasicProperties properties = new AMQP.BasicProperties();
        byte[] body = RandomUtil.randomShortByteArray();

        EzyRabbitMessageHandler handler = mock(EzyRabbitMessageHandler.class);

        EzyRabbitTopicServer sut = EzyRabbitTopicServer.builder()
            .channel(channel)
            .exchange(exchange)
            .queueName(queueName)
            .build();
        sut.setMessageHandler(handler);

        Consumer consumer = FieldUtil.getFieldValue(sut, "consumer");

        // when
        consumer.handleDelivery(consumerTag, envelope, properties, body);

        // then
        verify(handler, times(1)).handle(any());
    }
}
