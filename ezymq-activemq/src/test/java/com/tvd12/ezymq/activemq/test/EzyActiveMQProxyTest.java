package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.activemq.EzyActiveMQProxy;
import com.tvd12.ezymq.activemq.EzyActiveRpcConsumer;
import com.tvd12.ezymq.activemq.EzyActiveRpcProducer;
import com.tvd12.ezymq.activemq.EzyActiveTopic;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveConnectionFactory;
import com.tvd12.ezymq.activemq.manager.EzyActiveRpcConsumerManager;
import com.tvd12.ezymq.activemq.manager.EzyActiveRpcProducerManager;
import com.tvd12.ezymq.activemq.manager.EzyActiveTopicManager;
import com.tvd12.ezymq.activemq.setting.EzyActiveSettings;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

import javax.jms.ConnectionFactory;

import static org.mockito.Mockito.*;

public class EzyActiveMQProxyTest extends BaseTest {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void test() {
        // given
        EzyActiveSettings settings = EzyActiveSettings.builder()
            .build();
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyActiveTopicManager topicManagerTest = mock(EzyActiveTopicManager.class);
        EzyActiveTopic topic = mock(EzyActiveTopic.class);
        when(topicManagerTest.getTopic("test")).thenReturn(topic);

        EzyActiveRpcProducer producer = mock(EzyActiveRpcProducer.class);
        EzyActiveRpcProducerManager producerManagerTest = mock(EzyActiveRpcProducerManager.class);
        when(producerManagerTest.getRpcProducer("test")).thenReturn(producer);

        EzyActiveRpcConsumer consumer = mock(EzyActiveRpcConsumer.class);
        EzyActiveRpcConsumerManager consumerManagerTest = mock(EzyActiveRpcConsumerManager.class);
        when(consumerManagerTest.getRpcConsumer("test")).thenReturn(consumer);

        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

        // when
        EzyActiveMQProxy sut = new EzyActiveMQProxy(
            settings,
            dataCodec,
            entityCodec,
            connectionFactory
        ) {
            @Override
            protected EzyActiveTopicManager newTopicManager() {
                return topicManagerTest;
            }

            @Override
            protected EzyActiveRpcProducerManager newRpcProducerManager() {
                return producerManagerTest;
            }

            @Override
            protected EzyActiveRpcConsumerManager newActiveRpcConsumerManager() {
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
        EzyActiveSettings settings = EzyActiveSettings.builder()
            .build();
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        ConnectionFactory connectionFactory = new EzyActiveConnectionFactory();

        // when
        EzyActiveMQProxy sut = new EzyActiveMQProxy(
            settings,
            dataCodec,
            entityCodec,
            connectionFactory
        );

        // then
        sut.close();
    }
}
