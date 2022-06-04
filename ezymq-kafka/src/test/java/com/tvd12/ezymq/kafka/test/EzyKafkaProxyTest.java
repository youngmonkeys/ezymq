package com.tvd12.ezymq.kafka.test;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.kafka.EzyKafkaConsumer;
import com.tvd12.ezymq.kafka.EzyKafkaProducer;
import com.tvd12.ezymq.kafka.EzyKafkaProxy;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.manager.EzyKafkaConsumerManager;
import com.tvd12.ezymq.kafka.manager.EzyKafkaProducerManager;
import com.tvd12.ezymq.kafka.setting.EzyKafkaSettings;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class EzyKafkaProxyTest extends BaseTest {

    @Test
    public void test() {
        // given
        EzyKafkaSettings settings = EzyKafkaSettings.builder()
            .build();
        EzyKafkaDataCodec dataCodec = mock(EzyKafkaDataCodec.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        // when
        EzyKafkaProxy sut = new EzyKafkaProxy(
            settings,
            dataCodec,
            entityCodec
        );

        // then
        Asserts.assertThatThrows(
            () -> sut.getProducer("not found")
        ).isEqualsType(IllegalArgumentException.class);

        Asserts.assertThatThrows(
            () -> sut.getConsumer("not found")
        ).isEqualsType(IllegalArgumentException.class);

        sut.close();
    }

    @Test
    public void getEndpointTest() {
        // given
        EzyKafkaSettings settings = EzyKafkaSettings.builder()
            .build();
        EzyKafkaDataCodec dataCodec = mock(EzyKafkaDataCodec.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyKafkaProducer producer = mock(EzyKafkaProducer.class);
        EzyKafkaProducerManager producerManagerTest = mock(EzyKafkaProducerManager.class);
        when(producerManagerTest.getProducer("test")).thenReturn(producer);

        EzyKafkaConsumer consumer = mock(EzyKafkaConsumer.class);
        EzyKafkaConsumerManager consumerManagerTest = mock(EzyKafkaConsumerManager.class);
        when(consumerManagerTest.getConsumer("test")).thenReturn(consumer);

        // when
        EzyKafkaProxy sut = new EzyKafkaProxy(
            settings,
            dataCodec,
            entityCodec
        ) {
            @Override
            protected EzyKafkaProducerManager newProducerManager() {
                return producerManagerTest;
            }

            @Override
            protected EzyKafkaConsumerManager newConsumerManager() {
                return consumerManagerTest;
            }
        };

        // then
        Asserts.assertEquals(sut.getProducer("test"), producer);
        Asserts.assertEquals(sut.getConsumer("test"), consumer);

        verify(producerManagerTest, times(1)).getProducer("test");
        verify(consumerManagerTest, times(1)).getConsumer("test");
    }
}
