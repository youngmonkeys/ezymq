package com.tvd12.ezymq.mosquitto.test;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.mosquitto.EzyMosquittoProxy;
import com.tvd12.ezymq.mosquitto.EzyMosquittoRpcConsumer;
import com.tvd12.ezymq.mosquitto.EzyMosquittoRpcProducer;
import com.tvd12.ezymq.mosquitto.EzyMosquittoTopic;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.ezymq.mosquitto.manager.EzyMosquittoRpcConsumerManager;
import com.tvd12.ezymq.mosquitto.manager.EzyMosquittoRpcProducerManager;
import com.tvd12.ezymq.mosquitto.manager.EzyMosquittoTopicManager;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoSettings;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class EzyMosquittoProxyTest extends BaseTest {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void test() {
        // given
        EzyMosquittoSettings settings = EzyMosquittoSettings.builder()
            .build();
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyMosquittoTopicManager topicManagerTest = mock(EzyMosquittoTopicManager.class);
        EzyMosquittoTopic topic = mock(EzyMosquittoTopic.class);
        when(topicManagerTest.getTopic("test")).thenReturn(topic);

        EzyMosquittoRpcProducer producer = mock(EzyMosquittoRpcProducer.class);
        EzyMosquittoRpcProducerManager producerManagerTest = mock(EzyMosquittoRpcProducerManager.class);
        when(producerManagerTest.getRpcProducer("test")).thenReturn(producer);

        EzyMosquittoRpcConsumer consumer = mock(EzyMosquittoRpcConsumer.class);
        EzyMosquittoRpcConsumerManager consumerManagerTest = mock(EzyMosquittoRpcConsumerManager.class);
        when(consumerManagerTest.getRpcConsumer("test")).thenReturn(consumer);

        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);

        // when
        EzyMosquittoProxy sut = new EzyMosquittoProxy(
            mqttClient,
            settings,
            dataCodec,
            entityCodec
        ) {
            @Override
            protected EzyMosquittoTopicManager newTopicManager() {
                return topicManagerTest;
            }

            @Override
            protected EzyMosquittoRpcProducerManager newRpcProducerManager() {
                return producerManagerTest;
            }

            @Override
            protected EzyMosquittoRpcConsumerManager newMosquittoRpcConsumerManager() {
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
        EzyMosquittoSettings settings = EzyMosquittoSettings.builder()
            .build();
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);

        // when
        EzyMosquittoProxy sut = new EzyMosquittoProxy(
            mqttClient,
            settings,
            dataCodec,
            entityCodec
        );

        // then
        sut.close();
    }
}
