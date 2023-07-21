package com.tvd12.ezymq.mosquitto.test.manager;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttCallback;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.ezymq.mosquitto.manager.EzyMosquittoRpcProducerManager;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoRpcProducerSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class EzyMosquittoRpcProducerManagerTest extends BaseTest {

    @Test
    public void test() throws Exception {
        // given
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        String topic = RandomUtil.randomShortAlphabetString();
        String replyTopic = RandomUtil.randomShortAlphabetString();
        EzyMosquittoRpcProducerSetting producerSetting = EzyMosquittoRpcProducerSetting
            .builder()
            .topic(topic)
            .replyTopic(replyTopic)
            .build();
        String producerName = RandomUtil.randomShortAlphabetString();
        Map<String, EzyMosquittoRpcProducerSetting> rpcProducerSettings =
            EzyMapBuilder.mapBuilder()
                .put(producerName, producerSetting)
                .toMap();

        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);

        // when
        EzyMosquittoRpcProducerManager sut = new EzyMosquittoRpcProducerManager(
            mqttClient,
            entityCodec,
            rpcProducerSettings
        );

        // then
        Asserts.assertNotNull(
            sut.getRpcProducer(producerName)
        );
        Asserts.assertThatThrows(() -> sut.getRpcProducer("not found"))
                .isEqualsType(IllegalArgumentException.class);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verify(mqttClient, times(1)).subscribe(replyTopic);
        verifyNoMoreInteractions(mqttClient);
        verifyNoMoreInteractions(entityCodec);
        sut.close();
    }

    @Test
    public void createRpcProducerFailed() throws Exception {
        // given
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        String topic = RandomUtil.randomShortAlphabetString();
        String replyTopic = RandomUtil.randomShortAlphabetString();
        EzyMosquittoRpcProducerSetting producerSetting = EzyMosquittoRpcProducerSetting
            .builder()
            .topic(topic)
            .replyTopic(replyTopic)
            .build();
        String producerName = RandomUtil.randomShortAlphabetString();
        Map<String, EzyMosquittoRpcProducerSetting> rpcProducerSettings =
            EzyMapBuilder.mapBuilder()
                .put(producerName, producerSetting)
                .toMap();

        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);
        RuntimeException exception = new RuntimeException("test");
        doThrow(exception).when(mqttClient).subscribe(replyTopic);

        // when
        Throwable e = Asserts.assertThrows(() -> {
            EzyMosquittoRpcProducerManager sut = new EzyMosquittoRpcProducerManager(
                mqttClient,
                entityCodec,
                rpcProducerSettings
            );
            sut.close();
        });

        // then
        Asserts.assertEqualsType(e, IllegalStateException.class);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verify(mqttClient, times(1)).subscribe(replyTopic);
        verifyNoMoreInteractions(mqttClient);
        verifyNoMoreInteractions(entityCodec);
    }
}
