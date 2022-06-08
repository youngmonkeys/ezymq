package com.tvd12.ezymq.rabbitmq.test;

import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.rabbitmq.EzyRabbitMQProxy;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcConsumer;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcProducer;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitConnectionFactory;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitRpcConsumerManager;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitRpcProducerManager;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitTopicManager;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitSettings;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class EzyRabbitMQProxyTest extends BaseTest {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void test() {
        // given
        EzyRabbitSettings settings = EzyRabbitSettings.builder()
            .build();
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyRabbitTopicManager topicManagerTest = mock(EzyRabbitTopicManager.class);
        EzyRabbitTopic topic = mock(EzyRabbitTopic.class);
        when(topicManagerTest.getTopic("test")).thenReturn(topic);

        EzyRabbitRpcProducer producer = mock(EzyRabbitRpcProducer.class);
        EzyRabbitRpcProducerManager producerManagerTest = mock(EzyRabbitRpcProducerManager.class);
        when(producerManagerTest.getRpcProducer("test")).thenReturn(producer);

        EzyRabbitRpcConsumer consumer = mock(EzyRabbitRpcConsumer.class);
        EzyRabbitRpcConsumerManager consumerManagerTest = mock(EzyRabbitRpcConsumerManager.class);
        when(consumerManagerTest.getRpcConsumer("test")).thenReturn(consumer);

        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

        // when
        EzyRabbitMQProxy sut = new EzyRabbitMQProxy(
            settings,
            dataCodec,
            entityCodec,
            connectionFactory
        ) {
            @Override
            protected EzyRabbitTopicManager newTopicManager() {
                return topicManagerTest;
            }

            @Override
            protected EzyRabbitRpcProducerManager newRpcProducerManager() {
                return producerManagerTest;
            }

            @Override
            protected EzyRabbitRpcConsumerManager newRabbitRpcConsumerManager() {
                return consumerManagerTest;
            }
        };

        // then
        Asserts.assertEquals(sut.getTopic("test"), topic);
        Asserts.assertEquals(sut.getRpcProducer("test"), producer);
        Asserts.assertEquals(sut.getRpcConsumer("test"), consumer);

        verify(topicManagerTest, times(1)).getTopic("test");
        verify(producerManagerTest, times(1)).getRpcProducer("test");
        verify(consumerManagerTest, times(1)).getRpcConsumer("test");

        sut.close();
        verify(topicManagerTest, times(1)).close();
        verify(producerManagerTest, times(1)).close();
        verify(consumerManagerTest, times(1)).close();
    }

    @Test
    public void closeConnectionFactoryTest() {
        // given
        EzyRabbitSettings settings = EzyRabbitSettings.builder()
            .build();
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        ConnectionFactory connectionFactory = new EzyRabbitConnectionFactory();

        // when
        EzyRabbitMQProxy sut = new EzyRabbitMQProxy(
            settings,
            dataCodec,
            entityCodec,
            connectionFactory
        );

        // then
        sut.close();
    }
}
